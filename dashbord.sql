-- dashbord.sql

WITH
    last_paid_click AS (
        SELECT
            t.visitor_id,
            t.visit_date,
            t.utm_source,
            t.utm_medium,
            t.utm_campaign,
            t.lead_id,
            t.created_at,
            t.amount,
            t.closing_reason,
            t.status_id
        FROM (
            SELECT
                s.visitor_id,
                l.lead_id,
                l.created_at,
                l.amount,
                l.closing_reason,
                l.status_id,
                s.visit_date::timestamp AS visit_date,
                LOWER(s.source) AS utm_source,
                LOWER(s.medium) AS utm_medium,
                LOWER(s.campaign) AS utm_campaign,
                ROW_NUMBER() OVER (
                    PARTITION BY l.lead_id
                    ORDER BY
                        s.visit_date DESC,
                        LOWER(s.source) ASC,
                        LOWER(s.medium) ASC,
                        LOWER(s.campaign) ASC,
                        s.visitor_id DESC
                ) AS rn
            FROM leads AS l
            JOIN sessions AS s
                ON l.visitor_id = s.visitor_id
               AND s.visit_date <= l.created_at
            WHERE LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
        ) AS t
        WHERE t.rn = 1
    ),

    sessions_norm AS (
        SELECT
            s.visitor_id,
            s.visit_date::timestamp AS visit_ts,
            s.visit_date::date AS visit_date,
            LOWER(s.source) AS utm_source,
            LOWER(s.medium) AS utm_medium,
            LOWER(s.campaign) AS utm_campaign
        FROM sessions AS s
        WHERE LOWER(s.medium) IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
    ),

    sessions_ranked AS (
        SELECT
            sn.*,
            ROW_NUMBER() OVER (
                PARTITION BY sn.visitor_id, sn.visit_date
                ORDER BY
                    sn.visit_ts DESC,
                    sn.utm_source ASC,
                    sn.utm_medium ASC,
                    sn.utm_campaign ASC,
                    sn.visitor_id DESC
            ) AS rn
        FROM sessions_norm AS sn
    ),

    last_paid_sessions AS (
        SELECT
            sr.visitor_id,
            sr.visit_date,
            sr.utm_source,
            sr.utm_medium,
            sr.utm_campaign
        FROM sessions_ranked AS sr
        WHERE sr.rn = 1
    ),

    visitors_from_leads_ranked AS (
        SELECT
            lpc.visitor_id,
            lpc.visit_date::date AS visit_date,
            lpc.utm_source,
            lpc.utm_medium,
            lpc.utm_campaign,
            lpc.created_at,
            ROW_NUMBER() OVER (
                PARTITION BY lpc.visitor_id, lpc.visit_date::date
                ORDER BY
                    lpc.created_at DESC,
                    lpc.visit_date DESC,
                    lpc.utm_source ASC,
                    lpc.utm_medium ASC,
                    lpc.utm_campaign ASC
            ) AS rn
        FROM last_paid_click AS lpc
    ),

    visitors_from_leads AS (
        SELECT
            vflr.visitor_id,
            vflr.visit_date,
            vflr.utm_source,
            vflr.utm_medium,
            vflr.utm_campaign
        FROM visitors_from_leads_ranked AS vflr
        WHERE vflr.rn = 1
    ),

    has_lead_today AS (
        SELECT DISTINCT
            lpc.visitor_id,
            lpc.visit_date::date AS visit_date
        FROM last_paid_click AS lpc
    ),

    visitors_without_leads_today AS (
        SELECT
            lps.visitor_id,
            lps.visit_date,
            lps.utm_source,
            lps.utm_medium,
            lps.utm_campaign
        FROM last_paid_sessions AS lps
        LEFT JOIN has_lead_today AS hlt
            ON lps.visitor_id = hlt.visitor_id
           AND lps.visit_date = hlt.visit_date
        WHERE hlt.visitor_id IS NULL
    ),

    visitors_all AS (
        SELECT
            vfl.visitor_id,
            vfl.visit_date,
            vfl.utm_source,
            vfl.utm_medium,
            vfl.utm_campaign
        FROM visitors_from_leads AS vfl
        UNION ALL
        SELECT
            vwl.visitor_id,
            vwl.visit_date,
            vwl.utm_source,
            vwl.utm_medium,
            vwl.utm_campaign
        FROM visitors_without_leads_today AS vwl
    ),

    visits_agg AS (
        SELECT
            v.visit_date,
            v.utm_source,
            v.utm_medium,
            v.utm_campaign,
            COUNT(*) AS visitors_count
        FROM visitors_all AS v
        GROUP BY
            v.visit_date,
            v.utm_source,
            v.utm_medium,
            v.utm_campaign
    ),

    leads_agg AS (
        SELECT
            lpc.visit_date::date AS visit_date,
            lpc.utm_source,
            lpc.utm_medium,
            lpc.utm_campaign,
            COUNT(DISTINCT lpc.lead_id) AS leads_count,
            COUNT(
                DISTINCT CASE
                    WHEN lpc.closing_reason = 'Успешно реализовано'
                      OR lpc.status_id = 142
                    THEN lpc.lead_id
                END
            ) AS purchases_count,
            SUM(
                CASE
                    WHEN lpc.closing_reason = 'Успешно реализовано'
                      OR lpc.status_id = 142
                    THEN lpc.amount
                END
            ) AS revenue
        FROM last_paid_click AS lpc
        GROUP BY
            lpc.visit_date::date,
            lpc.utm_source,
            lpc.utm_medium,
            lpc.utm_campaign
    ),

    ads_raw AS (
        SELECT
            vk.campaign_date,
            vk.utm_source,
            vk.utm_medium,
            vk.utm_campaign,
            vk.daily_spent
        FROM vk_ads AS vk
        UNION ALL
        SELECT
            ya.campaign_date,
            ya.utm_source,
            ya.utm_medium,
            ya.utm_campaign,
            ya.daily_spent
        FROM ya_ads AS ya
    ),

    ads_union AS (
        SELECT
            ar.campaign_date::date AS visit_date,
            LOWER(ar.utm_source) AS utm_source,
            LOWER(ar.utm_medium) AS utm_medium,
            LOWER(ar.utm_campaign) AS utm_campaign,
            SUM(ar.daily_spent) AS total_cost
        FROM ads_raw AS ar
        WHERE LOWER(ar.utm_source) IN ('yandex', 'vk')
        GROUP BY
            ar.campaign_date::date,
            LOWER(ar.utm_source),
            LOWER(ar.utm_medium),
            LOWER(ar.utm_campaign)
    )

SELECT
    v.visit_date,
    v.utm_source,
    v.utm_medium,
    v.utm_campaign,
    v.visitors_count,
    COALESCE(au.total_cost, 0) AS total_cost,
    COALESCE(l.leads_count, 0) AS leads_count,
    COALESCE(l.purchases_count, 0) AS purchases_count,
    COALESCE(l.revenue, 0) AS revenue,
    -- per-row metrics (day slice)
    COALESCE(au.total_cost, 0) / NULLIF(v.visitors_count, 0) AS cpu,
    COALESCE(au.total_cost, 0) / NULLIF(COALESCE(l.leads_count, 0), 0) AS cpl,
    COALESCE(au.total_cost, 0) / NULLIF(COALESCE(l.purchases_count, 0), 0) AS cppu,
    (COALESCE(l.revenue, 0) - COALESCE(au.total_cost, 0)) / NULLIF(COALESCE(au.total_cost, 0), 0) * 100 AS roi_percent,
    COALESCE(l.leads_count, 0) / NULLIF(v.visitors_count, 0) AS cr_visit_to_lead,
    COALESCE(l.purchases_count, 0) / NULLIF(COALESCE(l.leads_count, 0), 0) AS cr_lead_to_buy
FROM visits_agg AS v
LEFT JOIN leads_agg AS l
    ON v.visit_date = l.visit_date
   AND v.utm_source = l.utm_source
   AND v.utm_medium = l.utm_medium
   AND v.utm_campaign = l.utm_campaign
LEFT JOIN ads_union AS au
    ON v.visit_date = au.visit_date
   AND v.utm_source = au.utm_source
   AND v.utm_medium = au.utm_medium
   AND v.utm_campaign = au.utm_campaign
ORDER BY
    v.visit_date ASC,
    v.visitors_count DESC,
    v.utm_source ASC,
    v.utm_medium ASC,
    v.utm_campaign ASC
;
