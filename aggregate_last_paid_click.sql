
WITH paid_sessions AS (
  SELECT
      s.visitor_id,
      s.visit_date::timestamp AS visit_ts,
      s.visit_date::date      AS visit_date,
      s.source   AS utm_source,
      s.medium   AS utm_medium,
      s.campaign AS utm_campaign
  FROM sessions s
  WHERE s.medium IN ('cpc','cpm','cpa','youtube','cpp','tg','social')
),
leads_ps AS (
  SELECT
      l.lead_id,
      l.visitor_id,
      l.created_at,
      l.amount,
      l.closing_reason,
      l.status_id,
      ps.visit_ts,
      ps.visit_date,
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
),
leads_lpc AS (
  SELECT
      lead_id,
      visitor_id,
      created_at,
      amount,
      closing_reason,
      status_id,
      visit_ts,
      visit_date,
      utm_source,
      utm_medium,
      utm_campaign
  FROM leads_ps
  WHERE rn = 1
),
visits_agg AS (
  SELECT
      p.visit_date,
      p.utm_source,
      p.utm_medium,
      p.utm_campaign,
      COUNT(*) AS visitors_count
  FROM paid_sessions p
  GROUP BY p.visit_date, p.utm_source, p.utm_medium, p.utm_campaign
),
leads_agg AS (
  SELECT
      llpc.visit_date,
      llpc.utm_source,
      llpc.utm_medium,
      llpc.utm_campaign,
      COUNT(DISTINCT llpc.lead_id) AS leads_count,
      COUNT(DISTINCT CASE
          WHEN llpc.closing_reason IN ('Успешная продажа','Успешно реализовано')
            OR llpc.status_id = 142
          THEN llpc.lead_id
      END) AS purchases_count,
      SUM(CASE
          WHEN llpc.closing_reason IN ('Успешная продажа','Успешно реализовано')
            OR llpc.status_id = 142
          THEN llpc.amount ELSE 0
      END) AS revenue
  FROM leads_lpc llpc
  GROUP BY llpc.visit_date, llpc.utm_source, llpc.utm_medium, llpc.utm_campaign
),
ads_union AS (
  SELECT
      campaign_date::date AS visit_date,
      utm_source,
      utm_medium,
      utm_campaign,
      SUM(daily_spent) AS total_cost
  FROM (
      SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
      UNION ALL
      SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
  ) u
  GROUP BY campaign_date::date, utm_source, utm_medium, utm_campaign
),
keys AS (
  SELECT visit_date, utm_source, utm_medium, utm_campaign FROM visits_agg
  UNION
  SELECT visit_date, utm_source, utm_medium, utm_campaign FROM leads_agg
  UNION
  SELECT visit_date, utm_source, utm_medium, utm_campaign FROM ads_union
)
SELECT
    k.visit_date                                  AS visit_date,
    COALESCE(v.visitors_count, 0)                 AS visitors_count,
    k.utm_source,
    k.utm_medium,
    k.utm_campaign,
    COALESCE(a.total_cost, 0)                     AS total_cost,
    COALESCE(l.leads_count, 0)                    AS leads_count,
    COALESCE(l.purchases_count, 0)                AS purchases_count,
    COALESCE(l.revenue, 0)                        AS revenue
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
    visit_date ASC,                -- от ранних к поздним
    visitors_count DESC,           -- по убыванию
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC,
    revenue DESC NULLS LAST;       -- от большего к меньшему, NULL последними
