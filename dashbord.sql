-- dashbord.sql

with
-- 1) Последний платный клик до лида
last_paid_click_base as (
    select
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        s.visitor_id,
        s.visit_date::timestamp as visit_date,
        lower(s.source)   as utm_source,
        lower(s.medium)   as utm_medium,
        lower(s.campaign) as utm_campaign,
        row_number() over (
            partition by l.lead_id
            order by
                s.visit_date desc,
                lower(s.source) asc,
                lower(s.medium) asc,
                lower(s.campaign) asc,
                s.visitor_id desc
        ) as rn
    from leads as l
    join sessions as s
        on l.visitor_id = s.visitor_id
       and l.created_at >= s.visit_date
       and lower(s.medium) in (
           'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
       )
),
last_paid_click as (
    select
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        lead_id,
        created_at,
        amount,
        closing_reason,
        status_id
    from last_paid_click_base
    where rn = 1
),

-- 2) Нормализованные сессии и последняя платная сессия в день
sessions_norm as (
    select
        s.visitor_id,
        s.visit_date::timestamp as visit_ts,
        s.visit_date::date      as visit_date,
        lower(s.source)   as utm_source,
        lower(s.medium)   as utm_medium,
        lower(s.campaign) as utm_campaign
    from sessions as s
    where lower(s.medium) in (
        'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
    )
),
sessions_ranked as (
    select
        sn.*,
        row_number() over (
            partition by sn.visitor_id, sn.visit_date
            order by
                sn.visit_ts desc,
                sn.utm_source asc,
                sn.utm_medium asc,
                sn.utm_campaign asc,
                sn.visitor_id desc
        ) as rn
    from sessions_norm as sn
),
last_paid_sessions as (
    select
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    from sessions_ranked
    where rn = 1
),

-- 3) Посетители по лидам и без лида в тот день
visitors_from_leads_ranked as (
    select
        lpc.visitor_id,
        lpc.visit_date::date as visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        lpc.created_at,
        row_number() over (
            partition by lpc.visitor_id, lpc.visit_date::date
            order by
                lpc.created_at desc,
                lpc.visit_date desc,
                lpc.utm_source asc,
                lpc.utm_medium asc,
                lpc.utm_campaign asc
        ) as rn
    from last_paid_click as lpc
),
visitors_from_leads as (
    select
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    from visitors_from_leads_ranked
    where rn = 1
),
has_lead_today as (
    select distinct
        lpc.visitor_id,
        lpc.visit_date::date as visit_date
    from last_paid_click as lpc
),
visitors_without_leads_today as (
    select
        lps.visitor_id,
        lps.visit_date,
        lps.utm_source,
        lps.utm_medium,
        lps.utm_campaign
    from last_paid_sessions as lps
    left join has_lead_today as hlt
        on lps.visitor_id = hlt.visitor_id
       and lps.visit_date = hlt.visit_date
    where hlt.visitor_id is null
),

-- 4) Все посетители и агрегаты
visitors_all as (
    select
        vfl.visitor_id,
        vfl.visit_date,
        vfl.utm_source,
        vfl.utm_medium,
        vfl.utm_campaign
    from visitors_from_leads as vfl

    union all

    select
        vwl.visitor_id,
        vwl.visit_date,
        vwl.utm_source,
        vwl.utm_medium,
        vwl.utm_campaign
    from visitors_without_leads_today as vwl
),
visits_agg as (
    select
        v.visit_date,
        v.utm_source,
        v.utm_medium,
        v.utm_campaign,
        count(*) as visitors_count
    from visitors_all as v
    group by
        v.visit_date,
        v.utm_source,
        v.utm_medium,
        v.utm_campaign
),
leads_agg as (
    select
        lpc.visit_date::date as visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        count(distinct lpc.lead_id) as leads_count,
        count(
            distinct case
                when lpc.closing_reason = 'Успешно реализовано'
                  or lpc.status_id = 142
                then lpc.lead_id
            end
        ) as purchases_count,
        sum(
            case
                when lpc.closing_reason = 'Успешно реализовано'
                  or lpc.status_id = 142
                then lpc.amount
            end
        ) as revenue
    from last_paid_click as lpc
    group by
        lpc.visit_date::date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign
),

-- 5) Расходы рекламы
ads_raw as (
    select
        vk.campaign_date,
        vk.utm_source,
        vk.utm_medium,
        vk.utm_campaign,
        vk.daily_spent
    from vk_ads as vk

    union all

    select
        ya.campaign_date,
        ya.utm_source,
        ya.utm_medium,
        ya.utm_campaign,
        ya.daily_spent
    from ya_ads as ya
),
ads_union as (
    select
        ar.campaign_date::date as visit_date,
        lower(ar.utm_source)   as utm_source,
        lower(ar.utm_medium)   as utm_medium,
        lower(ar.utm_campaign) as utm_campaign,
        sum(ar.daily_spent)    as total_cost
    from ads_raw as ar
    where lower(ar.utm_source) in ('yandex', 'vk')
    group by
        ar.campaign_date::date,
        lower(ar.utm_source),
        lower(ar.utm_medium),
        lower(ar.utm_campaign)
)

select
    v.visit_date,
    v.utm_source,
    v.utm_medium,
    v.utm_campaign,
    v.visitors_count,
    coalesce(au.total_cost, 0)     as total_cost,
    coalesce(l.leads_count, 0)     as leads_count,
    coalesce(l.purchases_count, 0) as purchases_count,
    coalesce(l.revenue, 0)         as revenue,
    -- метрики на строку (дневной срез)
    coalesce(au.total_cost, 0)
        / nullif(v.visitors_count, 0) as cpu,
    coalesce(au.total_cost, 0)
        / nullif(coalesce(l.leads_count, 0), 0) as cpl,
    coalesce(au.total_cost, 0)
        / nullif(coalesce(l.purchases_count, 0), 0) as cppu,
    (coalesce(l.revenue, 0) - coalesce(au.total_cost, 0))
        / nullif(coalesce(au.total_cost, 0), 0) * 100 as roi_percent,
    coalesce(l.leads_count, 0)
        / nullif(v.visitors_count, 0) as cr_visit_to_lead,
    coalesce(l.purchases_count, 0)
        / nullif(coalesce(l.leads_count, 0), 0) as cr_lead_to_buy
from visits_agg as v
left join leads_agg as l
    on v.visit_date   = l.visit_date
   and v.utm_source   = l.utm_source
   and v.utm_medium   = l.utm_medium
   and v.utm_campaign = l.utm_campaign
left join ads_union as au
    on v.visit_date   = au.visit_date
   and v.utm_source   = au.utm_source
   and v.utm_medium   = au.utm_medium
   and v.utm_campaign = au.utm_campaign
;

-- ========================= dataset A (основное) ==========================
