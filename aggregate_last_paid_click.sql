-- aggregate_last_paid_click — «как в примере», но отформатировано под sqlfluff

WITH tab AS (
    SELECT
        s.visitor_id,
        s.visit_date::timestamp AS visit_ts,
        s.visit_date::date AS visit_date,
        lower(s.source) AS utm_source,
        lower(s.medium) AS utm_medium,
        lower(s.campaign) AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        row_number() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC, s.visitor_id DESC
        ) AS rn
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON s.visitor_id = l.visitor_id
       AND s.visit_date <= l.created_at
    WHERE lower(s.medium) <> 'organic'
),

last_paid_click AS (
    SELECT
        t.visit_date,
        t.utm_source,
        t.utm_medium,
        t.utm_campaign,
        count(t.visitor_id) AS visitors_count,
        count(t.lead_id) AS leads_count,
        count(*) FILTER (WHERE t.status_id = 142) AS purchases_count,
        sum(t.amount) AS revenue
    FROM tab AS t
    WHERE t.rn = 1
    GROUP BY
        t.visit_date,
        t.utm_source,
        t.utm_medium,
        t.utm_campaign
),

ads AS (
    SELECT
        a.campaign_date::date AS campaign_date,
        lower(a.utm_source) AS utm_source,
        lower(a.utm_medium) AS utm_medium,
        lower(a.utm_campaign) AS utm_campaign,
        sum(a.daily_spent) AS total_cost
    FROM (
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM ya_ads
    ) AS a
    GROUP BY
        a.campaign_date::date,
        lower(a.utm_source),
        lower(a.utm_medium),
        lower(a.utm_campaign)
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
    ON lpv.visit_date = a.campaign_date
   AND lpv.utm_source = a.utm_source
   AND lpv.utm_medium = a.utm_medium
   AND lpv.utm_campaign = a.utm_campaign
ORDER BY
    lpv.visit_date ASC,
    lpv.visitors_count DESC,
    lpv.utm_source ASC,
    lpv.utm_medium ASC,
    lpv.utm_campaign ASC,
    lpv.revenue DESC NULLS LAST
LIMIT 15;
