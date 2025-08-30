with tab as (
    select
        s.visitor_id,
        s.visit_date::timestamp as visit_ts,
        s.visit_date::date as visit_date,
        lower(s.source) as utm_source,
        lower(s.medium) as utm_medium,
        lower(s.campaign) as utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        row_number() over (
            partition by s.visitor_id
            order by s.visit_date desc, s.visitor_id desc
        ) as rn
    from sessions as s
    left join leads as l
        on s.visitor_id = l.visitor_id
        and s.visit_date <= l.created_at
    where lower(s.medium) <> 'organic'
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
        sum(t.amount) as reven
