WITH last_paid_click AS (
  SELECT
    t.visitor_id,
    t.visit_date,
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
      s.visit_date::timestamp AS visit_date,
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
  		ORDER BY
    	s.visit_date    DESC,      -- самый поздний визит до лидa (<= created_at)
    	lower(s.source) ASC,       -- тай-брейк по алфавиту UTM
    	lower(s.medium) ASC,
    	lower(s.campaign) ASC,
    	s.visitor_id    DESC
		) AS rn
    FROM leads l
    JOIN sessions s
      ON s.visitor_id = l.visitor_id
     AND s.visit_date <= l.created_at      -- включительно!
     AND lower(s.medium) IN ('cpc','cpm','cpa','youtube','cpp','tg','social')
  ) t
  WHERE t.rn = 1
), sessions_norm AS (
  SELECT
    s.visitor_id,
    s.visit_date::timestamp AS visit_ts,
    s.visit_date::date      AS visit_date,
    lower(s.source)         AS utm_source,
    lower(s.medium)         AS utm_medium,
    lower(s.campaign)       AS utm_campaign
  FROM sessions s
WHERE lower(s.medium) IN ('cpc','cpm','cpa','youtube','cpp','tg','social')
), last_paid_sessions AS (                   -- 1 запись на посетителя в день (последний платный визит)
  SELECT visitor_id, visit_date, utm_source, utm_medium, utm_campaign
  FROM (
    SELECT
      sn.*,
	  ROW_NUMBER() OVER (
  		PARTITION BY sn.visitor_id, sn.visit_date
  		ORDER BY
    	sn.visit_ts   DESC,        -- самый поздний визит в день
    	sn.utm_source ASC,         -- тай-брейк по алфавиту
    	sn.utm_medium ASC,
    	sn.utm_campaign ASC,
    	sn.visitor_id DESC         -- окончательный детерминизм
		) AS rn
    FROM sessions_norm sn
  ) x
  WHERE x.rn = 1
), visitors_from_leads AS (
  SELECT
    t.visitor_id,
    t.visit_date,         
    t.utm_source,
    t.utm_medium,
    t.utm_campaign
  FROM (
    SELECT
      lpc.visitor_id,
      lpc.visit_date::date AS visit_date,
      lpc.utm_source,
      lpc.utm_medium,
      lpc.utm_campaign,
      lpc.created_at,
	  ROW_NUMBER() OVER (
  		PARTITION BY lpc.visitor_id, lpc.visit_date::date
  		ORDER BY lpc.created_at DESC,
           lpc.visit_date DESC,
           lpc.utm_source ASC, lpc.utm_medium ASC, lpc.utm_campaign ASC
		) AS rn
    FROM last_paid_click lpc
  ) t
  WHERE t.rn = 1
), visitors_without_leads_today AS (          -- те, у кого сегодня нет лида, но был платный визит
  SELECT lps.*
  FROM last_paid_sessions lps
  LEFT JOIN (
    SELECT DISTINCT visitor_id, visit_date::date AS visit_date
    FROM last_paid_click
  ) has_lead_today
    ON has_lead_today.visitor_id = lps.visitor_id
   AND has_lead_today.visit_date = lps.visit_date
  WHERE has_lead_today.visitor_id IS NULL
), visitors_all AS (                           -- все уникальные визитеры на день+UTM
  SELECT * FROM visitors_from_leads
  UNION
  SELECT * FROM visitors_without_leads_today
), visits_agg AS (
  SELECT
    v.visit_date,
    v.utm_source,
    v.utm_medium,
    v.utm_campaign,
    COUNT(*) AS visitors_count      -- по твоим требованиям: уникальные посетители
  FROM visitors_all v
  GROUP BY v.visit_date, v.utm_source, v.utm_medium, v.utm_campaign
), leads_agg AS (
  SELECT
    lpc.visit_date::date AS visit_date,
    lpc.utm_source,
    lpc.utm_medium,
    lpc.utm_campaign,
    COUNT(DISTINCT lpc.lead_id) AS leads_count,
    COUNT(DISTINCT CASE WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 THEN lpc.lead_id END) AS purchases_count,
    SUM(CASE WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 THEN lpc.amount END) AS revenue
  FROM last_paid_click lpc
  GROUP BY lpc.visit_date::date, lpc.utm_source, lpc.utm_medium, lpc.utm_campaign
), ads_union AS (
  SELECT
    a.campaign_date::date AS visit_date,
    lower(a.utm_source)   AS utm_source,
    lower(a.utm_medium)   AS utm_medium,
    lower(a.utm_campaign) AS utm_campaign,
    SUM(a.daily_spent)    AS total_cost
  FROM (
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
    UNION ALL
    SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
  ) a
  WHERE lower(a.utm_source) IN ('yandex','vk')   -- чтобы ключи совпадали с sessions
  GROUP BY a.campaign_date::date, lower(a.utm_source), lower(a.utm_medium), lower(a.utm_campaign)
)
SELECT
  v.visit_date,
  v.visitors_count,
  v.utm_source,
  v.utm_medium,
  v.utm_campaign,
  au.total_cost,
  l.leads_count,
  l.purchases_count,
  l.revenue
FROM visits_agg v
LEFT JOIN leads_agg l
  ON l.visit_date   = v.visit_date
 AND l.utm_source   = v.utm_source
 And l.utm_medium   = v.utm_medium
 And l.utm_campaign = v.utm_campaign
LEFT JOIN ads_union au
  ON au.visit_date   = v.visit_date
 AND au.utm_source   = v.utm_source
 AND au.utm_medium   = v.utm_medium
 AND au.utm_campaign = v.utm_campaign
ORDER BY
  v.visit_date ASC,            -- от ранних к поздним
  v.visitors_count DESC,       -- убыв.
  v.utm_source ASC,            -- алфавит
  v.utm_medium ASC,
  v.utm_campaign ASC,
  l.revenue DESC NULLS LAST    -- и ещё revenue в убыв., null — в конец (как в задании)
LIMIT 15;