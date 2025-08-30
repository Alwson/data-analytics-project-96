-- aggregate_last_paid_click — версия «как в примере», но аккуратно нормализована

WITH tab AS (
  SELECT
    s.visitor_id,
    s.visit_date::timestamp AS visit_ts,      -- точное время визита
    s.visit_date::date      AS visit_date,    -- дата визита
    lower(s.source)   AS utm_source,
    lower(s.medium)   AS utm_medium,
    lower(s.campaign) AS utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id,
    ROW_NUMBER() OVER (
      PARTITION BY s.visitor_id
      ORDER BY s.visit_date DESC, s.visitor_id DESC
    ) AS rn
  FROM sessions AS s
  LEFT JOIN leads AS l
    ON s.visitor_id = l.visitor_id
   AND s.visit_date <= l.created_at          -- визит до/на момент создания лида
  WHERE lower(s.medium) <> 'organic'         -- платный/соц. трафик, без органики
),
last_paid_click AS (
  SELECT
    t.visit_date,
    t.utm_source,
    t.utm_medium,
    t.utm_campaign,
    COUNT(t.visitor_id)                             AS visitors_count,
    COUNT(t.lead_id)                                AS leads_count,
    COUNT(*) FILTER (WHERE t.status_id = 142)       AS purchases_count,
    SUM(t.amount)                                   AS revenue
  FROM tab t
  WHERE t.rn = 1                                    -- только последний визит на посетителя
  GROUP BY
    t.visit_date, t.utm_source, t.utm_medium, t.utm_campaign
),
ads AS (
  SELECT
    a.campaign_date::date AS campaign_date,
    lower(a.utm_source)   AS utm_source,
    lower(a.utm_medium)   AS utm_medium,
    lower(a.utm_campaign) AS utm_campaign,
    SUM(a.daily_spent)    AS total_cost
  FROM (
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
    UNION ALL
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
  ) a
  GROUP BY a.campaign_date::date, lower(a.utm_source), lower(a.utm_medium), lower(a.utm_campaign)
)
SELECT
  lpv.visit_date,
  lpv.visitors_count,
  lpv.utm_source,
  lpv.utm_medium,
  lpv.utm_campaign,
  a.total_cost,
  lpv.leads_count,
  lpv.purchases_count,
  lpv.revenue
FROM last_paid_click AS lpv
LEFT JOIN ads AS a
  ON a.campaign_date = lpv.visit_date
 AND a.utm_source    = lpv.utm_source
 AND a.utm_medium    = lpv.utm_medium
 AND a.utm_campaign  = lpv.utm_campaign
ORDER BY
  lpv.revenue DESC NULLS LAST,  -- сперва самые «денежные»
  lpv.visit_date ASC,
  lpv.visitors_count DESC,
  lpv.utm_source ASC,
  lpv.utm_medium ASC,
  lpv.utm_campaign ASC
LIMIT 15;