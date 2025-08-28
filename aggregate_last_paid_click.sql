-- ===== aggregate_last_paid_click.sql (версия с учетом utm_content) =====

-- A) Расходы с детализацией по content (для фильтра) и без content (для суммы)
WITH ads_detail AS (
  SELECT
      campaign_date::date         AS visit_date,
      lower(utm_source)           AS utm_source,
      lower(utm_medium)           AS utm_medium,
      lower(utm_campaign)         AS utm_campaign,
      lower(utm_content)          AS utm_content,
      SUM(daily_spent)            AS total_cost_content
  FROM (
      SELECT campaign_date, utm_source, utm_medium, utm_campaign, utm_content, daily_spent FROM vk_ads
      UNION ALL
      SELECT campaign_date, utm_source, utm_medium, utm_campaign, utm_content, daily_spent FROM ya_ads
  ) u
  GROUP BY campaign_date::date, lower(utm_source), lower(utm_medium), lower(utm_campaign), lower(utm_content)
),
ads_union AS (
  SELECT
      visit_date,
      utm_source,
      utm_medium,
      utm_campaign,
      SUM(total_cost_content) AS total_cost
  FROM ads_detail
  GROUP BY visit_date, utm_source, utm_medium, utm_campaign
),
-- B) Платные сессии (включаем utm_content)
paid_sessions AS (
  SELECT
      s.visitor_id,
      s.visit_date::timestamp     AS visit_ts,
      s.visit_date::date          AS visit_date,
      lower(s.source)             AS utm_source,
      lower(s.medium)             AS utm_medium,
      lower(s.campaign)           AS utm_campaign,
      lower(s.content)            AS utm_content
  FROM sessions s
  WHERE lower(s.medium) IN ('cpc','cpm','cpa','youtube','cpp','tg','social')
),
-- C) Последний платный клик для посетителя в день
daily_last_click AS (
  SELECT *
  FROM (
    SELECT
      p.*,
      ROW_NUMBER() OVER (
        PARTITION BY p.visitor_id, p.visit_date
        ORDER BY p.visit_ts DESC
      ) AS rn_day
    FROM paid_sessions p
  ) t
  WHERE rn_day = 1
),
-- D) Оставляем только клики, для которых в этот день был расход по ТОЧНОМУ content
filtered_clicks AS (
  SELECT d.*
  FROM daily_last_click d
  JOIN ads_detail a
    ON a.visit_date   = d.visit_date
   AND a.utm_source   = d.utm_source
   AND a.utm_medium   = d.utm_medium
   AND a.utm_campaign = d.utm_campaign
   AND a.utm_content  = d.utm_content
  WHERE EXISTS (
    SELECT 1
    FROM leads l
    WHERE l.visitor_id = d.visitor_id
      AND l.created_at::date >= d.visit_date   -- лид после визита
  )
),
-- E) Агрегируем визиты (это уже «1 посетитель в день» за счёт CTE выше)
visits_agg AS (
  SELECT
      f.visit_date,
      f.utm_source,
      f.utm_medium,
      f.utm_campaign,
      COUNT(*) AS visitors_count
  FROM filtered_clicks f
  GROUP BY f.visit_date, f.utm_source, f.utm_medium, f.utm_campaign
),
-- F) Для атрибуции лидов берём ТОЛЬКО те платные визиты, где есть расход по content
eligible_paid_sessions AS (
  SELECT p.*
  FROM paid_sessions p
  WHERE EXISTS (
    SELECT 1
    FROM ads_detail a
    WHERE a.visit_date   = p.visit_date
      AND a.utm_source   = p.utm_source
      AND a.utm_medium   = p.utm_medium
      AND a.utm_campaign = p.utm_campaign
      AND a.utm_content  = p.utm_content
  )
),
-- G) Привязка лида к последнему «разрешённому» платному клику ДО/В created_at
leads_ps AS (
  SELECT
      l.lead_id,
      l.visitor_id,
      l.created_at,
      l.amount,
      l.closing_reason,
      l.status_id,
      eps.visit_ts,
      eps.visit_date,
      eps.utm_source,
      eps.utm_medium,
      eps.utm_campaign,
      eps.utm_content,
      ROW_NUMBER() OVER (
        PARTITION BY l.lead_id
        ORDER BY eps.visit_ts DESC
      ) AS rn
  FROM leads l
  JOIN eligible_paid_sessions eps
    ON eps.visitor_id = l.visitor_id
   AND eps.visit_ts  <= l.created_at
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
      -- utm_content дальше не нужен, он использован только для отбора
  FROM leads_ps
  WHERE rn = 1
),
-- H) Лиды/покупки/ревенью в разрезе ключа
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
-- I) Полный набор ключей для аккуратного merge
keys AS (
  SELECT visit_date, utm_source, utm_medium, utm_campaign FROM visits_agg
  UNION
  SELECT visit_date, utm_source, utm_medium, utm_campaign FROM leads_agg
  UNION
  SELECT visit_date, utm_source, utm_medium, utm_campaign FROM ads_union
)
-- J) Финальная витрина
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