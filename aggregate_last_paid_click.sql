-- aggregate_last_paid_click.sql

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
            l.lead_id,
            l.created_at,
            l.amount,
            l.closing_reason,
            l.status_id,
            s.visit_date::timestamp AS visit_date,
            LOWER(s.source) AS utm_source,
            LOWER(s.medium) AS utm_medium,
            LOWER(s.campaign) AS utm_campaign,
            ROW_NUMBER() OVER (
                PARTITION BY l.lead_id
                ORDER BY s.visit_date DESC
            ) AS rn
        FROM leads AS l
        INNER JOIN sessions AS s
            ON
                leads.visitor_id = sessions.visitor_id
                AND leads.created_at >= sessions.visit_date
        WHERE LOWER(s.medium) IN (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
        )
    ) AS t
    WHERE t.rn = 1
),

visits_agg AS (
    SELECT
        s.visit_date::date AS visit_date,
        LOWER(s.source) AS utm_source,
        LOWER(s.medium) AS utm_medium,
        LOWER(s.campaign) AS utm_campaign,
        COUNT(DISTINCT s.visitor_id) AS visitors_count
    FROM sessions AS s
    WHERE
        LOWER(s.medium) IN (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
        )
    GROUP BY
        s.visit_date::date,
        LOWER(s.source),
        LOWER(s.medium),
        LOWER(s.campaign)
),

leads_agg AS (
    SELECT
        lpc.visit_date::date AS visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        COUNT(DISTINCT lpc.lead_id) AS leads_count,
        COUNT(
            DISTINCT CASE
                WHEN
                    lpc.closing_reason = 'Успешно реализовано'
                    OR lpc.status_id = 142
                    THEN lpc.lead_id
            END
        ) AS purchases_count,
        SUM(
            CASE
                WHEN
                    lpc.closing_reason = 'Успешно реализовано'
                    OR lpc.status_id = 142
                    THEN lpc.amount
            END
        ) AS revenue
    FROM last_paid_click AS lpc
    GROUP BY
        lpc.visit_date::date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign
),

ads_union AS (
    SELECT
        u.campaign_date::date AS visit_date,
        LOWER(u.utm_source) AS utm_source,
        LOWER(u.utm_medium) AS utm_medium,
        LOWER(u.utm_campaign) AS utm_campaign,
        SUM(u.daily_spent) AS total_cost
    FROM (
        SELECT
            vk.campaign_date,
            vk.utm_source,
            vk.utm_medium,
            vk.utm_campaign,
            vk.daily_spent
        FROM vk_ads AS vk
        UNION ALL
        SELECT
            ya.campaign_date,
            ya.utm_source,
            ya.utm_medium,
            ya.utm_campaign,
            ya.daily_spent
        FROM ya_ads AS ya
    ) AS u
    GROUP BY
        u.campaign_date::date,
        LOWER(u.utm_source),
        LOWER(u.utm_medium),
        LOWER(u.utm_campaign)
)

SELECT
    v.visit_date,
    v.utm_source,
    v.utm_medium,
    v.utm_campaign,
    v.visitors_count,
    au.total_cost,
    l.leads_count,
    l.purchases_count,
    l.revenue
FROM visits_agg AS v
LEFT JOIN leads_agg AS l
    ON
        v.visit_date = l.visit_date
        AND v.utm_source = l.utm_source
        AND v.utm_medium = l.utm_medium
        AND v.utm_campaign = l.utm_campaign
LEFT JOIN
    ads_union AS au
    ON
        v.visit_date = au.visit_date
        AND v.utm_source = au.utm_source
        AND v.utm_medium = au.utm_medium
        AND v.utm_campaign = au.utm_campaign
ORDER BY
    v.visit_date ASC,
    v.utm_source ASC;

