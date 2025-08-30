-- aggregate_last_paid_click.sql — вариант, максимально близкий к тестам
WITH sessions_norm AS (
  SELECT
    s.visitor_id,
    s.visit_date::timestamp AS visit_ts,
    s.visit_date::date      AS visit_date,
    lower(s.source)         AS utm_source,
    lower(s.medium)         AS utm_medium,
    lower(s.campaign)       AS utm_campaign
  FROM sessions s
), last_paid_click AS (
  -- последний платный визит до created_at (TIMESTAMP)
  SELECT
    t.visitor_id,
    t.visit_ts,
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
      sn.visitor_id,
      sn.visit_ts,
      sn.utm_source,
      sn.utm_medium,
      sn.utm_campaign,
      l.lead_id,
      l.created_at,
      l.amount,
      l.closing_reason,
      l.status_id,
      ROW_NUMBER() OVER (
        PARTITION BY l.lead_id
        ORDER BY sn.visit_ts DESC
      ) AS rn
    FROM leads l
    JOIN sessions_norm sn
      ON sn.visitor_id = l.visitor_id
     AND sn.visit_ts   <= l.created_at
     AND sn.utm_medium IN ('cpc','cpm','cpa','youtube','cpp','tg','social')
  ) t
  WHERE t.rn = 1
), last_paid_sessions AS (
  -- последний платный визит человека в пределах дня
  SELECT
    x.visitor_id,
    x.visit_date,
    x.utm_source,
    x.utm_medium,
    x.utm_campaign
  FROM (
    SELECT
      sn.visitor_id,
      sn.visit_date,
      sn.utm_source,
      sn.utm_medium,
      sn.utm_campaign,
      ROW_NUMBER() OVER (
        PARTITION BY sn.visitor_id, sn.visit_date
        ORDER BY sn.visit_ts DESC
      ) AS rn
    FROM sessions_norm sn
    WHERE sn.utm_medium IN ('cpc','cpm','cpa','youtube','cpp','tg','social')
  ) x
  WHERE x.rn = 1
), visitors_from_leads AS (
  -- у кого в этот день был лид → берём метки из last_paid_click
  SELECT DISTINCT
    lpc.visitor_id,
    lpc.visit_ts::date AS visit_date,
    lpc.utm_source,
    lpc.utm_medium,
    lpc.utm_campaign
  FROM last_paid_click lpc
), visitors_without_leads_today AS (
  -- у кого в этот день не было лида → берём их последний платный визит дня
  SELECT lps.*
  FROM last_paid_sessions lps
  LEFT JOIN (
    SELECT DISTINCT visitor_id, visit_ts::date AS visit_date
    FROM last_paid_click
  ) has_lead_today
    ON has_lead_today.visitor_id = lps.visitor_id
   AND has_lead_today.visit_date = lps.visit_date
  WHERE has_lead_today.visitor_id IS NULL
), visitors_all AS (
  SELECT visitor_id, visit_date, utm_source, utm_medium, utm_campaign FROM visitors_from_leads
  UNION
  SELECT visitor_id, visit_date, utm_source, utm_medium, utm_campaign FROM visitors_without_leads_today
), visits_agg AS (
  SELECT
      v.visit_date,
      v.utm_source,
      v.utm_medium,
      v.utm_campaign,
      COUNT(DISTINCT v.visitor_id) AS visitors_count
  FROM visitors_all v
  GROUP BY v.visit_date, v.utm_source, v.utm_medium, v.utm_campaign
), leads_agg AS (
  SELECT
      lpc.visit_ts::date AS visit_date,
      lpc.utm_source,
      lpc.utm_medium,
      lpc.utm_campaign,
      COUNT(DISTINCT lpc.lead_id) AS leads_count,
      COUNT(DISTINCT CASE WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 THEN lpc.lead_id END) AS purchases_count,
      SUM(CASE WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 THEN lpc.amount END) AS revenue
  FROM last_paid_click lpc
  GROUP BY lpc.visit_ts::date, lpc.utm_source, lpc.utm_medium, lpc.utm_campaign
), ads_union AS (
  -- расходы, нормализуем через lower(...)
  SELECT
      u.campaign_date::date AS visit_date,
      lower(u.utm_source)   AS utm_source,
      lower(u.utm_medium)   AS utm_medium,
      lower(u.utm_campaign) AS utm_campaign,
      SUM(u.daily_spent)    AS total_cost
  FROM (
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
    UNION ALL
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
  ) u
  GROUP BY u.campaign_date::date, lower(u.utm_source), lower(u.utm_medium), lower(u.utm_campaign)
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