-- dashbord.sql

WITH metrics AS (
    SELECT
        v.utm_source,
        SUM(v.visitors_count) AS visitors,
        SUM(au.total_cost) AS total_cost,
        SUM(l.leads_count) AS leads,
        SUM(l.purchases_count) AS purchases,
        SUM(l.revenue) AS revenue
    FROM visits_agg AS v
    LEFT JOIN leads_agg AS l
        ON
            v.visit_date = l.visit_date
            AND v.utm_source = l.utm_source
            AND v.utm_medium = l.utm_medium
            AND v.utm_campaign = l.utm_campaign
    LEFT JOIN ads_union AS au
        ON
            v.visit_date = au.visit_date
            AND v.utm_source = au.utm_source
            AND v.utm_medium = au.utm_medium
            AND v.utm_campaign = au.utm_campaign
    GROUP BY
        v.utm_source
)

SELECT
    m.utm_source,
    m.visitors,
    m.total_cost,
    m.leads,
    m.purchases,
    m.revenue,
    m.total_cost / NULLIF(m.visitors, 0) AS cpu,
    m.total_cost / NULLIF(m.leads, 0) AS cpl,
    m.total_cost / NULLIF(m.purchases, 0) AS cppu,
    (m.revenue - m.total_cost) / NULLIF(m.total_cost, 0) * 100 AS roi_percent
FROM metrics AS m
ORDER BY
    roi_percent DESC NULLS LAST;
