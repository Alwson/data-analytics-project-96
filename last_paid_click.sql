-- last_paid_click.sql — упрощённая финальная версия (TOP-10)

SELECT
  t.visitor_id,
  t.visit_date,      -- TIMESTAMP из sessions
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
    s.visitor_id,
    s.visit_date::timestamp AS visit_date,          -- в CSV нужен timestamp
    lower(s.source)   AS utm_source,
    lower(s.medium)   AS utm_medium,
    lower(s.campaign) AS utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id,
    ROW_NUMBER() OVER (
      PARTITION BY l.lead_id
      ORDER BY s.visit_date DESC
    ) AS rn
  FROM leads l
  JOIN sessions s
    ON s.visitor_id = l.visitor_id
   AND s.visit_date <= l.created_at
   AND lower(s.medium) IN ('cpc','cpm','cpa','youtube','cpp','tg','social')
) t
WHERE t.rn = 1
ORDER BY
  t.amount DESC NULLS LAST,
  t.visit_date ASC,
  t.utm_source ASC,
  t.utm_medium ASC,
  t.utm_campaign ASC
LIMIT 10;
