WITH paid_sessions AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source   AS utm_source,
        s.medium   AS utm_medium,
        s.campaign AS utm_campaign,
        ROW_NUMBER() OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC) AS rn
    FROM sessions s
    WHERE s.medium IN ('cpc','cpm','cpa','youtube','cpp','tg','social')
),
last_paid_click AS (
    SELECT
        lpc.visitor_id,
        lpc.visit_date::date AS visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM paid_sessions lpc
    LEFT JOIN leads l
      ON l.visitor_id = lpc.visitor_id
     AND l.created_at >= lpc.visit_date
    WHERE rn = 1
),
ads_union AS (
    SELECT campaign_date::date AS visit_date,
           utm_source, utm_medium, utm_campaign,
           SUM(daily_spent) AS total_cost
    FROM (
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM vk_ads
        UNION ALL
        SELECT campaign_date, utm_source, utm_medium, utm_campaign, daily_spent FROM ya_ads
    ) u
    GROUP BY campaign_date::date, utm_source, utm_medium, utm_campaign
),
visits_agg AS (
    SELECT
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        COUNT(*) AS visitors_count,
        COUNT(lpc.lead_id) FILTER (WHERE lpc.lead_id IS NOT NULL) AS leads_count,
        COUNT(lpc.lead_id) FILTER (
            WHERE lpc.closing_reason IN ('Успешная продажа','Успешно реализовано')
               OR lpc.status_id = 142
        ) AS purchases_count,
        SUM(lpc.amount) FILTER (
            WHERE lpc.closing_reason IN ('Успешная продажа','Успешно реализовано')
               OR lpc.status_id = 142
        ) AS revenue
    FROM last_paid_click lpc
    GROUP BY lpc.visit_date, lpc.utm_source, lpc.utm_medium, lpc.utm_campaign
)
SELECT
    va.visit_date,
    va.visitors_count,
    va.utm_source,
    va.utm_medium,
    va.utm_campaign,
    COALESCE(au.total_cost, 0) AS total_cost,
    va.leads_count,
    va.purchases_count,
    va.revenue
FROM visits_agg va
LEFT JOIN ads_union au
  ON au.visit_date = va.visit_date
 AND au.utm_source = va.utm_source
 AND au.utm_medium = va.utm_medium
 AND au.utm_campaign = va.utm_campaign
ORDER BY
    va.visit_date ASC,
    va.visitors_count DESC,
    va.utm_source ASC,
    va.utm_medium ASC,
    va.utm_campaign ASC,
    va.revenue DESC NULLS LAST
LIMIT 15;
