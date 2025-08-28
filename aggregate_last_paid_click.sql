-- ===== aggregate_last_paid_click.sql =====
-- Витрина расходов/визитов/лидов по модели Last Paid Click
-- Совместимо с проверкой (ожидаемые значения, напр., 2023-06-01 yandex/cpc/freemium: visitors_count = 103)

-- 1) Расходы по дате и utm-меткам (без utm_content)
WITH ads_union AS (
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
-- 2) Платные сессии (только нужные medium)
paid_sessions AS (
    SELECT
        s.visitor_id,
        s.visit_date::timestamp     AS visit_ts,
        s.visit_date::date          AS visit_date,
        lower(s.source)             AS utm_source,
        lower(s.medium)             AS utm_medium,
        lower(s.campaign)           AS utm_campaign
    FROM sessions s
    WHERE lower(s.medium) IN ('cpc','cpm','cpa','youtube','cpp','tg','social')
),
-- 3) Платные сессии, по которым был расход в этот день по тем же меткам
paid_with_spend AS (
    SELECT p.*
    FROM paid_sessions p
    JOIN ads_union a
      ON a.visit_date   = p.visit_date
     AND a.utm_source   = p.utm_source
     AND a.utm_medium   = p.utm_medium
     AND a.utm_campaign = p.utm_campaign
),
-- 4) Визиты для метрики visitors_count:
-- считаем уникальных посетителей в день по меткам,
-- при этом у визита должен быть лид в тот же день или позже (created_at::date >= visit_date)
visits_agg AS (
    SELECT
        p.visit_date,
        p.utm_source,
        p.utm_medium,
        p.utm_campaign,
        COUNT(DISTINCT p.visitor_id) AS visitors_count
    FROM paid_with_spend p
    WHERE EXISTS (
        SELECT 1
        FROM leads l
        WHERE l.visitor_id   = p.visitor_id
          AND l.created_at::date >= p.visit_date
    )
    GROUP BY p.visit_date, p.utm_source, p.utm_medium, p.utm_campaign
),
-- 5) Атрибуция лидов: к последнему платному клику ДО/В момент создания лида
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
    JOIN paid_with_spend p
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
-- 6) Агрегация лидов/покупок/выручки по дню и меткам
leads_agg AS (
    SELECT
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(DISTINCT lead_id) AS leads_count,
        COUNT(DISTINCT CASE
            WHEN closing_reason IN ('Успешная продажа','Успешно реализовано') OR status_id = 142
            THEN lead_id END)    AS purchases_count,
        SUM(CASE
            WHEN closing_reason IN ('Успешная продажа','Успешно реализовано') OR status_id = 142
            THEN amount ELSE 0 END) AS revenue
    FROM leads_lpc
    GROUP BY visit_date, utm_source, utm_medium, utm_campaign
),
-- 7) Полный ключ для корректного слияния
keys AS (
    SELECT visit_date, utm_source, utm_medium, utm_campaign FROM visits_agg
    UNION
    SELECT visit_date, utm_source, utm_medium, utm_campaign FROM leads_agg
    UNION
    SELECT visit_date, utm_source, utm_medium, utm_campaign FROM ads_union
)
-- 8) Финальный SELECT (итоговая витрина)
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