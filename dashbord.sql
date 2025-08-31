-- dashbord.sql

WITH
-- 1) последний платный клик до лида
last_paid_click_base AS (
    SELECT
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        s.visitor_id,
        s.visit_date::timestamp AS visit_date,
        lower(s.source) AS utm_source,
        lower(s.medium) AS utm_medium,
        lower(s.campaign) AS utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY l.lead_id
            ORDER BY
                s.visit_date DESC,
                lower(s.source) ASC,
                lower(s.medium) ASC,
                lower(s.campaign) ASC,
                s.visitor_id DESC
        ) AS rn
    FROM leads AS l
    INNER JOIN sessions AS s
        ON l.visitor_id = s.visitor_id
       AND l.created_at >= s.visit_date
       AND lower(s.medium) IN (
           'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
       )
),

last_paid_click AS (
    SELECT
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        lead_id,
        created_at,
        amount,
        closing_reason,
        status_id
    FROM last_paid_click_base
    WHERE rn = 1
),

-- 2) нормализованные сессии и последняя платная сессия в день
sessions_norm AS (
    SELECT
        s.visitor_id,
        s.visit_date::timestamp AS visit_ts,
        s.visit_date::date AS visit_date,
        lower(s.source) AS utm_source,
        lower(s.medium) AS utm_medium,
        lower(s.campaign) AS utm_campaign
    FROM sessions AS s
    WHERE lower(s.medium) IN (
        'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
    )
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
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    FROM sessions_ranked
    WHERE rn = 1
),

-- 3) посетители по лидам и без лида в тот день
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
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    FROM visitors_from_leads_ranked
    WHERE rn = 1
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

-- 4) все посетители и агрегаты
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

-- 5) расходы рекламы
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
        lower(ar.utm_source) AS utm_source,
        lower(ar.utm_medium) AS utm_medium,
        lower(ar.utm_campaign) AS utm_campaign,
        SUM(ar.daily_spent) AS total_cost
    FROM ads_raw AS ar
    WHERE lower(ar.utm_source) IN ('yandex', 'vk')
    GROUP BY
        ar.campaign_date::date,
        lower(ar.utm_source),
        lower(ar.utm_medium),
        lower(ar.utm_campaign)
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
    -- метрики на строку (дневной срез)
    COALESCE(au.total_cost, 0)
        / NULLIF(v.visitors_count, 0) AS cpu,
    COALESCE(au.total_cost, 0)
        / NULLIF(COALESCE(l.leads_count, 0), 0) AS cpl,
    COALESCE(au.total_cost, 0)
        / NULLIF(COALESCE(l.purchases_count, 0), 0) AS cppu,
    (COALESCE(l.revenue, 0) - COALESCE(au.total_cost, 0))
        / NULLIF(COALESCE(au.total_cost, 0), 0) * 100 AS roi_percent,
    COALESCE(l.leads_count, 0)
        / NULLIF(v.visitors_count, 0) AS cr_visit_to_lead,
    COALESCE(l.purchases_count, 0)
        / NULLIF(COALESCE(l.leads_count, 0), 0) AS cr_lead_to_buy
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
;

-- ========================= dataset A (основное) ==========================
