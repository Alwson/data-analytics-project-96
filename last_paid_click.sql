-- last_paid_click.sql — финальная версия под тест (TOP-10)

WITH paid_sessions AS (
  SELECT
      s.visitor_id,
      s.visit_date::timestamp AS visit_ts,
      s.visit_date::timestamp AS visit_date,  -- в CSV нужен timestamp
      lower(s.source)   AS utm_source,
      lower(s.medium)   AS utm_medium,
      lower(s.campaign) AS utm_campaign
  FROM sessions s
  WHERE lower(s.medium) IN ('cpc','cpm','cpa','youtube','cpp','tg','social')  -- ВСЕ платные
)
, leads_ps AS (
  SELECT
      l.lead_id,
      l.visitor_id,
      l.created_at,
      l.amount,
      l.closing_reason,
      l.status_id,
      ps.visit_ts,
      ps.visit_date,              -- timestamp
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
      lead_id, visitor_id, created_at, amount, closing_reason, status_id,
      visit_ts, visit_date, utm_source, utm_medium, utm_campaign
  FROM leads_ps
  WHERE rn = 1
)
SELECT
    ps.visitor_id,
    ps.visit_date,         -- TIMESTAMP
    ps.utm_source,
    ps.utm_medium,
    ps.utm_campaign,
    ll.lead_id,
    ll.created_at,
    ll.amount,
    ll.closing_reason,
    ll.status_id
FROM paid_sessions ps
LEFT JOIN leads_lpc ll
  ON ll.visitor_id   = ps.visitor_id
 AND ll.visit_ts     = ps.visit_ts   -- привязываем к конкретному визиту
 AND ll.utm_source   = ps.utm_source
 AND ll.utm_medium   = ps.utm_medium
 AND ll.utm_campaign = ps.utm_campaign
ORDER BY
    ll.amount DESC NULLS LAST,
    ps.visit_date ASC,
    ps.utm_source ASC,
    ps.utm_medium ASC,
    ps.utm_campaign ASC
LIMIT 10;