WITH paid_sessions AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source   AS utm_source,
        s.medium   AS utm_medium,
        s.campaign AS utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id 
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions s
    WHERE s.medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
last_paid_click AS (
    SELECT *
    FROM paid_sessions
    WHERE rn = 1
)
SELECT
    lpc.visitor_id,
    lpc.visit_date,
    lpc.utm_source,
    lpc.utm_medium,
    lpc.utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
FROM last_paid_click lpc
LEFT JOIN leads l
    ON l.visitor_id = lpc.visitor_id
   AND l.created_at >= lpc.visit_date
ORDER BY
    l.amount DESC NULLS LAST,
    lpc.visit_date ASC,
    lpc.utm_source ASC,
    lpc.utm_medium ASC,
    lpc.utm_campaign ASC;