-- aggregate_last_paid_click.sql — агрегированная витрина (TOP-15) по правилам пользователя
WITH paid_mediums AS (
  SELECT unnest(ARRAY['cpc','cpm','cpa','youtube','cpp','tg','social']) AS m
), paid_sessions AS (
  SELECT
      s.visitor_id,
      s.visit_date::date AS visit_date,
      lower(trim(s.source))   AS utm_source,
      lower(trim(s.medium))   AS utm_medium,
      lower(trim(s.campaign)) AS utm_campaign
  FROM sessions s
  JOIN paid_mediums pm ON lower(trim(s.medium)) = pm.m
), visits_agg AS (
  SELECT
      visit_date,
      utm_source,
      utm_medium,
      utm_campaign,
      COUNT(DISTINCT visitor_id) AS visitors_count
  FROM paid_sessions
  GROUP BY visit_date, utm_source, utm_medium, utm_campaign
), last_paid_click AS (
  SELECT
      l.lead_id,
      l.visitor_id,
      l.created_at,
      l.amount,
      l.closing_reason,
      l.status_id,
      ps.visit_date,
      ps.utm_source,
      ps.utm_medium,
      ps.utm_campaign,
      ROW_NUMBER() OVER (PARTITION BY l.lead_id ORDER BY ps.visit_date DESC) AS rn
  FROM leads l
  JOIN paid_sessions ps
    ON ps.visitor_id = l.visitor_id
   AND ps.visit_date <= l.created_at::date
), leads_agg AS (
  SELECT
      visit_date,
      utm_source,
      utm_medium,
      utm_campaign,
      COUNT(DISTINCT lead_id) AS leads_count,
      COUNT(DISTINCT CASE
        WHEN closing_reason = 'Успешно реализовано' OR status_id = 142
        THEN lead_id END) AS purchases_count,
      SUM(CASE
        WHEN closing_reason = 'Успешно реализовано' OR status_id = 142
        THEN amount ELSE NULL END) AS revenue
  FROM last_paid_click
  WHERE rn = 1
  GROUP BY visit_date, utm_source, utm_medium, utm_campaign
), ads_union AS (
  SELECT
      campaign_date::date AS visit_date,
      lower(trim(utm_source))   AS utm_source,
      lower(trim(utm_medium))   AS utm_medium,
      lower(trim(utm_campaign)) AS utm_campaign,
      SUM(daily_spent) AS total_cost
  FROM (
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
    UNION ALL
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
  ) u
  GROUP BY campaign_date::date, lower(trim(utm_source)), lower(trim(utm_medium)), lower(trim(utm_campaign))
)
SELECT
    v.visit_date,
    v.visitors_count,
    v.utm_source,
    v.utm_medium,
    v.utm_campaign,
    a.total_cost,
    l.leads_count,
    l.purchases_count,
    l.revenue
FROM visits_agg v
LEFT JOIN leads_agg l
  ON l.visit_date   = v.visit_date
 AND l.utm_source   = v.utm_source
 AND l.utm_medium   = v.utm_medium
 AND l.utm_campaign = v.utm_campaign
LEFT JOIN ads_union a
  ON a.visit_date   = v.visit_date
 AND a.utm_source   = v.utm_source
 AND a.utm_medium   = v.utm_medium
 AND a.utm_campaign = v.utm_campaign
ORDER BY
    v.visit_date ASC,
    v.visitors_count DESC,
    v.utm_source ASC,
    v.utm_medium ASC,
    v.utm_campaign ASC,
    l.revenue DESC NULLS LAST
LIMIT 15;