-- ===== aggregate_last_paid_click.sql
WITH paid_sessions AS (
    SELECT
        s.visitor_id,
        s.visit_date::timestamp AS visit_ts,
        s.visit_date::date      AS visit_date,
        lower(s.source)         AS utm_source,
        lower(s.medium)         AS utm_medium,
        lower(s.campaign)       AS utm_campaign
    FROM sessions s
    WHERE lower(s.medium) IN ('cpc','cpm','cpa','youtube','cpp','tg','social')
),
ads_union AS (
    SELECT
        campaign_date::date        AS visit_date,
        lower(utm_source)          AS utm_source,
        lower(utm_medium)          AS utm_medium,
        lower(utm_campaign)        AS utm_campaign,
        SUM(daily_spent)           AS total_cost
    FROM (
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
        UNION ALL
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
    ) u
    GROUP BY campaign_date::date, lower(utm_source), lower(utm_medium), lower(utm_campaign)
),
-- ВИЗИТЫ: считаем уникальных посетителей за день по UTM
visits_agg AS (
    SELECT
        p.visit_date,
        p.utm_source,
        p.utm_medium,
        p.utm_campaign,
        COUNT(DISTINCT p.visitor_id) AS visitors_count
    FROM paid_sessions p
    GROUP BY p.visit_date, p.utm_source, p.utm_medium, p.utm_campaign
),
-- Атрибуция лидов: последний платный визит до/в created_at
leads_ps AS (
    SELECT
        l.lead_id,
        l.visitor_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        p.visit_ts,
        p.visit_date,
        p.utm_source,
        p.utm_medium,
        p.utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY l.lead_id
            ORDER BY p.visit_ts DESC
        ) AS rn
    FROM leads l
    JOIN paid_sessions p
      ON p.visitor_id = l.visitor_id
     AND p.visit_ts   <= l.created_at
),
leads_lpc AS (
    SELECT
        lead_id,
        visitor_id,
        created_at,
        amount,
        closing_reason,
        status_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    FROM leads_ps
    WHERE rn = 1
),
-- Лиды/покупки/выручка
leads_agg AS (
    SELECT
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(DISTINCT lead_id) AS leads_count,
        COUNT(DISTINCT CASE
            WHEN closing_reason = 'Успешно реализовано' OR status_id = 142
            THEN lead_id END)   AS purchases_count,
        SUM(CASE
            WHEN closing_reason = 'Успешно реализовано' OR status_id = 142
            THEN amount ELSE 0 END) AS revenue
    FROM leads_lpc
    GROUP BY visit_date, utm_source, utm_medium, utm_campaign
),
keys AS (
    SELECT visit_date, utm_source, utm_medium, utm_campaign FROM visits_agg
    UNION
    SELECT visit_date, utm_source, utm_medium, utm_campaign FROM leads_agg
    UNION
    SELECT visit_date, utm_source, utm_medium, utm_campaign FROM ads_union
)
SELECT
    k.visit_date,
    COALESCE(v.visitors_count,  0) AS visitors_count,
    k.utm_source,
    k.utm_medium,
    k.utm_campaign,
    COALESCE(a.total_cost,      0) AS total_cost,
    COALESCE(l.leads_count,     0) AS leads_count,
    COALESCE(l.purchases_count, 0) AS purchases_count,
    COALESCE(l.revenue,         0) AS revenue
FROM keys k
LEFT JOIN visits_agg v
  ON v.visit_date   = k.visit_date
 AND v.utm_source   = k.utm_source
 AND v.utm_medium   = k.utm_medium
 AND v.utm_campaign = k.utm_campaign
LEFT JOIN leads_agg l
  ON l.visit_date   = k.visit_date
 AND l.utm_source   = k.utm_source
 AND l.utm_medium   = k.utm_medium
 AND l.utm_campaign = k.utm_campaign
LEFT JOIN ads_union a
  ON a.visit_date   = k.visit_date
 AND a.utm_source   = k.utm_source
 AND a.utm_medium   = k.utm_medium
 AND a.utm_campaign = k.utm_campaign
ORDER BY
    k.visit_date ASC,
    COALESCE(v.visitors_count, 0) DESC,
    k.utm_source ASC,
    k.utm_medium ASC,
    k.utm_campaign ASC,

    COALESCE(l.revenue, 0) DESC NULLS LAST;
