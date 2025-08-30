-- aggregate_last_paid_click.sql

with tab as (
    select
        -- простые поля сначала (ST06)
        s.visitor_id,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        -- вычисляемые поля ниже
        s.visit_date::timestamp as visit_ts,
        s.visit_date::date as visit_date,
        lower(s.source) as utm_source,
        lower(s.medium) as utm_medium,
        lower(s.campaign) as utm_campaign,
        row_number() over (
            partition by s.visitor_id
            order by
                s.visit_date desc,
                s.visitor_id desc
        ) as rn
    from
        sessions as s
    left join
        leads as l
        on s.visitor_id = l.visitor_id
        and s.visit_date <= coalesce(l.created_at, s.visit_date)
    where
        lower(s.medium) <> 'organic'
),

last_paid_click as (
    select
        t.visit_date,
        t.utm_source,
        t.utm_medium,
        t.utm_campaign,
        count(t.visitor_id) as visitors_count,
        count(t.lead_id) as leads_count,
        count(*) filter (where t.status_id = 142) as purchases_count,
        sum(t.amount) as revenue
    from
        tab as t
    where
        t.rn = 1
    group by
        t.visit_date,
        t.utm_source,
        t.utm_medium,
        t.utm_campaign
),

ads as (
    select
        a.campaign_date::date as campaign_date,
        lower(a.utm_source) as utm_source,
        lower(a.utm_medium) as utm_medium,
        lower(a.utm_campaign) as utm_campaign,
        sum(a.daily_spent) as total_cost
    from (
        select
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        from
            vk_ads
        union all
        select
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        from
            ya_ads
    ) as a
    group by
        a.campaign_date::date,
        lower(a.utm_source),
        lower(a.utm_medium),
        lower(a.utm_campaign)
)

select
    lpv.visit_date,
    lpv.visitors_count,
    lpv.utm_source,
    lpv.utm_medium,
    lpv.utm_campaign,
    a.total_cost,
    lpv.leads_count,
    lpv.purchases_count,
    lpv.revenue
from
    last_paid_click as lpv
left join
    ads as a
    on lpv.visit_date = a.campaign_date
    and lpv.utm_source = a.utm_source
    and lpv.utm_medium = a.utm_medium
    and lpv.utm_campaign = a.utm_campaign
order by
    lpv.visit_date asc,
    lpv.visitors_count desc,
    lpv.utm_source asc,
    lpv.utm_medium asc,
    lpv.utm_campaign asc,
    lpv.revenue desc nulls last
limit 15;
