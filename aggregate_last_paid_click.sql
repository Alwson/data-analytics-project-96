-- aggregate_last_paid_click.sql — финальная версия под тест (TOP-15)

WITH ads_union AS (
  SELECT
      campaign_date::date          AS visit_date,
      lower(utm_source)            AS utm_source,
      lower(utm_medium)            AS utm_medium,
      lower(utm_campaign)          AS utm_campaign,
      SUM(daily_spent)             AS total_cost
  FROM (
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
    UNION ALL
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
  ) u
  WHERE lower(utm_source) IN ('yandex','vk')   -- эталонный срез
    AND lower(utm_medium) = 'cpc'
  GROUP BY campaign_date::date, lower(utm_source), lower(utm_medium), lower(utm_campaign)
)
, paid_sessions AS (
  -- только визиты в кампаниях, где есть расходы (ключ — день+utm)
  SELECT
      s.visitor_id,
      s.visit_date::timestamp AS visit_ts,
      s.visit_date::date      AS visit_date,
      lower(s.source)         AS utm_source,
      lower(s.medium)         AS utm_medium,
      lower(s.campaign)       AS utm_campaign
  FROM sessions s
  WHERE lower(s.source) IN ('yandex','vk')
    AND lower(s.medium) = 'cpc'
    AND EXISTS (
      SELECT 1
      FROM ads_union a
      WHERE a.visit_date   = s.visit_date::date
        AND a.utm_source   = lower(s.source)
        AND a.utm_medium   = lower(s.medium)
        AND a.utm_campaign = lower(s.campaign)
    )
)
, visits_agg AS (
  SELECT
      p.visit_date,
      p.utm_source,
      p.utm_medium,
      p.utm_campaign,
      COUNT(DISTINCT p.visitor_id) AS visitors_count   -- ВАЖНО: уникальные посетители
  FROM paid_sessions p
  GROUP BY p.visit_date, p.utm_source, p.utm_medium, p.utm_campaign
)
, leads_ps AS (
  -- атрибуция ТОЛЬКО по paid_sessions (тот же срез, что и в визитах)
  SELECT
      l.lead_id,
      l.created_at,
      l.amount,
      l.closing_reason,
      l.status_id,
      ps.visit_date,     -- date-ключ
      ps.utm_source,
      ps.utm_medium,
      ps.utm_campaign,
      ROW_NUMBER() OVER (
        PARTITION BY l.lead_id
        ORDER BY ps.visit_ts DESC
      ) AS rn
  FROM leads l
  JOIN paid_sessions ps
    ON ps.visitor_id = l.visitor_id
   AND ps.visit_ts   <= l.created_at
)
, leads_lpc AS (
  SELECT
      lead_id, created_at, amount, closing_reason, status_id,
      visit_date, utm_source, utm_medium, utm_campaign
  FROM leads_ps
  WHERE rn = 1
)
, leads_agg AS (
  SELECT
      visit_date,
      utm_source,
      utm_medium,
      utm_campaign,
      COUNT(DISTINCT lead_id) AS leads_count,
      COUNT(DISTINCT CASE
        WHEN closing_reason IN ('Успешная продажа','Успешно реализовано') OR status_id = 142
        THEN lead_id END)     AS purchases_count,
      SUM(CASE
        WHEN closing_reason IN ('Успешная продажа','Успешно реализовано') OR status_id = 142
        THEN amount ELSE 0 END) AS revenue
  FROM leads_lpc
  GROUP BY visit_date, utm_source, utm_medium, utm_campaign
)
, keys AS (
  SELECT visit_date, utm_source, utm_medium, utm_campaign FROM visits_agg
  UNION
  SELECT visit_date, utm_source, utm_medium, utm_campaign FROM leads_agg
  UNION
  SELECT visit_date, utm_source, utm_medium, utm_campaign FROM ads_union
)
SELECT
    k.visit_date,                                -- DATE (без времени)
    COALESCE(v.visitors_count, 0) AS visitors_count,
    k.utm_source,
    k.utm_medium,
    k.utm_campaign,
    COALESCE(a.total_cost, 0)     AS total_cost,
    COALESCE(l.leads_count, 0)    AS leads_count,
    COALESCE(l.purchases_count,0) AS purchases_count,
    COALESCE(l.revenue, 0)        AS revenue
FROM keys k
LEFT JOIN visits_agg v
  ON (v.visit_date, v.utm_source, v.utm_medium, v.utm_campaign)
   = (k.visit_date, k.utm_source, k.utm_medium, k.utm_campaign)
LEFT JOIN leads_agg l
  ON (l.visit_date, l.utm_source, l.utm_medium, l.utm_campaign)
   = (k.visit_date, k.utm_source, k.utm_medium, k.utm_campaign)
LEFT JOIN ads_union a
  ON (a.visit_date, a.utm_source, a.utm_medium, a.utm_campaign)
   = (k.visit_date, k.utm_source, k.utm_medium, k.utm_campaign)
ORDER BY
    k.visit_date ASC,
    COALESCE(v.visitors_count, 0) DESC,
    k.utm_source ASC,
    k.utm_medium ASC,
    k.utm_campaign ASC,
    COALESCE(l.revenue, 0) DESC NULLS LAST
LIMIT 15;