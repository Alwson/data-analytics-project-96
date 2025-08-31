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
            lower(s.source) AS utm_source,
            lower(s.medium) AS utm_medium,
            lower(s.campaign) AS utm_campaign,
            l.lead_id,
            l.created_at,
            l.amount,
            l.closing_reason,
            l.status_id,
            ROW_NUMBER() OVER (
                PARTITION BY l.lead_id
                ORDER BY
                    s.visit_date DESC,
                    lower(s.source) ASC,
                    lower(s.medium) ASC,
                    lower(s.campaign) ASC,
                    s.visitor_id DESC
            ) AS rn
        FROM leads AS l
        JOIN sessions AS s
            ON s.visitor_id = l.visitor_id
           AND s.visit_date <= l.created_at
           AND lower(s.medium) IN (
               'cpc','cpm','cpa','youtube','cpp','tg','social'
           )
    ) AS t
    WHERE t.rn = 1
),
sessions_norm AS (
    SELECT
        s.visitor_id,
        s.visit_date::timestamp AS visit_ts,
        s.visit_date::date AS visit_date,
        lower(s.source) AS utm_source,
        lower(s.medium) AS utm_medium,
        lower(s.campaign) AS utm_campaign
    FROM sessions AS s
    WHERE lower(s.medium) IN (
        'cpc','cpm','cpa','youtube','cpp','tg','social'
    )
),
last_paid_sessions AS (
    SELECT
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    FROM (
        SELECT
            sn.*,
            ROW_NUMBER() OVER (
                PARTITION BY sn.visitor_id, sn.visit_date
                ORDER BY
                    sn.visit_ts DESC,
                    sn.utm_source ASC,
                    sn.utm_medium ASC,
                    sn.utm_campaign ASC,
                    sn.visitor_id DESC
            ) AS rn
        FROM sessions_norm AS sn
    ) AS x
    WHERE x.rn = 1
),
visitors_from_leads AS (
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
                ORDER BY
                    lpc.created_at DESC,
                    lpc.visit_date DESC,
                    lpc.utm_source ASC,
                    lpc.utm_medium ASC,
                    lpc.utm_campaign ASC
            ) AS rn
        FROM last_paid_click AS lpc
    ) AS t
    WHERE t.rn = 1
),
visitors_without_leads_today AS (
    SELECT lps.*
    FROM last_paid_sessions AS lps
    LEFT JOIN (
        SELECT DISTINCT
            visitor_id,
            visit_date::date AS visit_date
        FROM last_paid_click
    ) AS has_lead_today
        ON has_lead_today.visitor_id = lps.visitor_id
       AND has_lead_today.visit_date = lps.visit_date
    WHERE has_lead_today.visitor_id IS NULL
),
visitors_all AS (
    SELECT * FROM visitors_from_leads
    UNION ALL
    SELECT * FROM visitors_without_leads_today
),
visits_agg AS (
    SELECT
        v.visit_date,
        v.utm_source,
        v.utm_medium,
        v.utm_campaign,
        COUNT(*) AS visitors_count
    FROM visitors_all AS v
    GROUP BY
        v.visit_date, v.utm_source, v.utm_medium, v.utm_campaign
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
                WHEN lpc.closing_reason = 'Успешно реализовано'
                  OR lpc.status_id = 142
                THEN lpc.lead_id END
        ) AS purchases_count,
        SUM(
            CASE
                WHEN lpc.closing_reason = 'Успешно реализовано'
                  OR lpc.status_id = 142
                THEN lpc.amount END
        ) AS revenue
    FROM last_paid_click AS lpc
    GROUP BY
        lpc.visit_date::date,
        lpc.utm_source, lpc.utm_medium, lpc.utm_campaign
),
ads_union AS (
    SELECT
        a.campaign_date::date AS visit_date,
        lower(a.utm_source) AS utm_source,
        lower(a.utm_medium) AS utm_medium,
        lower(a.utm_campaign) AS utm_campaign,
        SUM(a.daily_spent) AS total_cost
    FROM (
        SELECT
            campaign_date, utm_source, utm_medium, utm_campaign, daily_spent
        FROM vk_ads
        UNION ALL
        SELECT
            campaign_date, utm_source, utm_medium, utm_campaign, daily_spent
        FROM ya_ads
    ) AS a
    WHERE lower(a.utm_source) IN ('yandex','vk')
    GROUP BY
        a.campaign_date::date,
        lower(a.utm_source),
        lower(a.utm_medium),
        lower(a.utm_campaign)
)
SELECT
    v.visit_date,
    v.utm_source,
    v.utm_medium,
    v.utm_campaign,
    v.visitors_count,
    COALESCE(au.total_cost, 0) AS total_cost,
    COALESCE(l.leads_count, 0) AS leads_count,
    COALESCE(l.purchases_count, 0) AS purchases_count,
    COALESCE(l.revenue, 0) AS revenue,
    /* метрики на строку (дневной срез) */
    COALESCE(au.total_cost, 0)
        / NULLIF(v.visitors_count, 0) AS cpu,
    COALESCE(au.total_cost, 0)
        / NULLIF(COALESCE(l.leads_count, 0), 0) AS cpl,
    COALESCE(au.total_cost, 0)
        / NULLIF(COALESCE(l.purchases_count, 0), 0) AS cppu,
    (COALESCE(l.revenue, 0) - COALESCE(au.total_cost, 0))
        / NULLIF(COALESCE(au.total_cost, 0), 0) * 100 AS roi_percent,
    COALESCE(l.leads_count, 0)
        / NULLIF(v.visitors_count, 0) AS cr_visit_to_lead,
    COALESCE(l.purchases_count, 0)
        / NULLIF(COALESCE(l.leads_count, 0), 0) AS cr_lead_to_buy
FROM visits_agg AS v
LEFT JOIN leads_agg AS l
    ON l.visit_date = v.visit_date
   AND l.utm_source = v.utm_source
   AND l.utm_medium = v.utm_medium
   AND l.utm_campaign = v.utm_campaign
LEFT JOIN ads_union AS au
    ON au.visit_date = v.visit_date
   AND au.utm_source = v.utm_source
   AND au.utm_medium = v.utm_medium
   AND au.utm_campaign = v.utm_campaign;
--- dataset A (основное)

WITH daily_smc AS (
    /* это ровно Dataset A без финальных метрик, только базовые поля */
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
                lower(s.source) AS utm_source,
                lower(s.medium) AS utm_medium,
                lower(s.campaign) AS utm_campaign,
                l.lead_id,
                l.created_at,
                l.amount,
                l.closing_reason,
                l.status_id,
                ROW_NUMBER() OVER (
                    PARTITION BY l.lead_id
                    ORDER BY
                        s.visit_date DESC,
                        lower(s.source) ASC,
                        lower(s.medium) ASC,
                        lower(s.campaign) ASC,
                        s.visitor_id DESC
                ) AS rn
            FROM leads AS l
            JOIN sessions AS s
                ON s.visitor_id = l.visitor_id
               AND s.visit_date <= l.created_at
               AND lower(s.medium) IN (
                   'cpc','cpm','cpa','youtube','cpp','tg','social'
               )
        ) AS t
        WHERE t.rn = 1
    ),
    sessions_norm AS (
        SELECT
            s.visitor_id,
            s.visit_date::timestamp AS visit_ts,
            s.visit_date::date AS visit_date,
            lower(s.source) AS utm_source,
            lower(s.medium) AS utm_medium,
            lower(s.campaign) AS utm_campaign
        FROM sessions AS s
        WHERE lower(s.medium) IN (
            'cpc','cpm','cpa','youtube','cpp','tg','social'
        )
    ),
    last_paid_sessions AS (
        SELECT
            visitor_id,
            visit_date,
            utm_source,
            utm_medium,
            utm_campaign
        FROM (
            SELECT
                sn.*,
                ROW_NUMBER() OVER (
                    PARTITION BY sn.visitor_id, sn.visit_date
                    ORDER BY
                        sn.visit_ts DESC,
                        sn.utm_source ASC,
                        sn.utm_medium ASC,
                        sn.utm_campaign ASC,
                        sn.visitor_id DESC
                ) AS rn
            FROM sessions_norm AS sn
        ) AS x
        WHERE x.rn = 1
    ),
visitors_from_leads AS (
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
                    ORDER BY
                        lpc.created_at DESC,
                        lpc.visit_date DESC,
                        lpc.utm_source ASC,
                        lpc.utm_medium ASC,
                        lpc.utm_campaign ASC
                ) AS rn
            FROM last_paid_click AS lpc
        ) AS t
        WHERE t.rn = 1
    ),
    visitors_without_leads_today AS (
        SELECT lps.*
        FROM last_paid_sessions AS lps
        LEFT JOIN (
            SELECT DISTINCT
                visitor_id,
                visit_date::date AS visit_date
            FROM last_paid_click
        ) AS has_lead_today
            ON has_lead_today.visitor_id = lps.visitor_id
           AND has_lead_today.visit_date = lps.visit_date
        WHERE has_lead_today.visitor_id IS NULL
    ),
    visitors_all AS (
        SELECT * FROM visitors_from_leads
        UNION ALL
        SELECT * FROM visitors_without_leads_today
    ),
    visits_agg AS (
        SELECT
            v.visit_date,
            v.utm_source,
            v.utm_medium,
            v.utm_campaign,
            COUNT(*) AS visitors_count
        FROM visitors_all AS v
        GROUP BY
            v.visit_date, v.utm_source, v.utm_medium, v.utm_campaign
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
                    WHEN lpc.closing_reason = 'Успешно реализовано'
                      OR lpc.status_id = 142
                    THEN lpc.lead_id END
            ) AS purchases_count,
            SUM(
                CASE
                    WHEN lpc.closing_reason = 'Успешно реализовано'
                      OR lpc.status_id = 142
                    THEN lpc.amount END
            ) AS revenue
        FROM last_paid_click AS lpc
        GROUP BY
            lpc.visit_date::date,
            lpc.utm_source, lpc.utm_medium, lpc.utm_campaign
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
                    WHEN lpc.closing_reason = 'Успешно реализовано'
                      OR lpc.status_id = 142
                    THEN lpc.lead_id END
            ) AS purchases_count,
            SUM(
                CASE
                    WHEN lpc.closing_reason = 'Успешно реализовано'
                      OR lpc.status_id = 142
                    THEN lpc.amount END
            ) AS revenue
        FROM last_paid_click AS lpc
        GROUP BY
            lpc.visit_date::date,
            lpc.utm_source, lpc.utm_medium, lpc.utm_campaign
    ),
    ads_union AS (
        SELECT
            a.campaign_date::date AS visit_date,
            lower(a.utm_source) AS utm_source,
            lower(a.utm_medium) AS utm_medium,
            lower(a.utm_campaign) AS utm_campaign,
            SUM(a.daily_spent) AS total_cost
        FROM (
            SELECT
                campaign_date, utm_source, utm_medium, utm_campaign, daily_spent
            FROM vk_ads
            UNION ALL
            SELECT
                campaign_date, utm_source, utm_medium, utm_campaign, daily_spent
            FROM ya_ads
        ) AS a
        WHERE lower(a.utm_source) IN ('yandex','vk')
        GROUP BY
            a.campaign_date::date,
            lower(a.utm_source),
            lower(a.utm_medium),
            lower(a.utm_campaign)
    )
    SELECT
        v.visit_date,
        v.utm_source,
        v.utm_medium,
        v.utm_campaign,
        v.visitors_count,
        COALESCE(au.total_cost, 0) AS total_cost,
        COALESCE(l.leads_count, 0) AS leads_count,
        COALESCE(l.purchases_count, 0) AS purchases_count,
        COALESCE(l.revenue, 0) AS revenue
    FROM visits_agg AS v
    LEFT JOIN leads_agg AS l
        ON l.visit_date = v.visit_date
       AND l.utm_source = v.utm_source
       AND l.utm_medium = v.utm_medium
       AND l.utm_campaign = v.utm_campaign
    LEFT JOIN ads_union AS au
        ON au.visit_date = v.visit_date
       AND au.utm_source = v.utm_source
       AND au.utm_medium = v.utm_medium
       AND au.utm_campaign = v.utm_campaign
),
agg_source AS (
    SELECT
        visit_date,
        utm_source,
        SUM(visitors_count) AS visitors_count,
        SUM(total_cost) AS total_cost,
        SUM(leads_count) AS leads_count,
        SUM(purchases_count) AS purchases_count,
        SUM(revenue) AS revenue
    FROM daily_smc
    GROUP BY visit_date, utm_source
)
SELECT
    visit_date,
    utm_source,
    visitors_count,
    total_cost,
    leads_count,
    purchases_count,
    revenue,
    /* корректные ratio-of-sums по источнику и дню */
    total_cost / NULLIF(visitors_count, 0) AS cpu,
    total_cost / NULLIF(leads_count, 0) AS cpl,
    total_cost / NULLIF(purchases_count, 0) AS cppu,
    (revenue - total_cost) / NULLIF(total_cost, 0) * 100 AS roi_percent,
    leads_count / NULLIF(visitors_count, 0) AS cr_visit_to_lead,
    purchases_count / NULLIF(leads_count, 0) AS cr_lead_to_buy
FROM agg_source;
-- dataset B

