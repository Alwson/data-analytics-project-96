-- last_paid_click.sql — упрощённая финальная версия (TOP-10)

select
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
from (
    select
        -- ST06: сначала простые поля
        s.visitor_id,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id,
        -- затем вычисляемые
        s.visit_date::timestamp as visit_date,
        lower(s.source) as utm_source,
        lower(s.medium) as utm_medium,
        lower(s.campaign) as utm_campaign,
        row_number() over (
            partition by l.lead_id
            order by s.visit_date desc
        ) as rn
    from leads as l
    inner join sessions as s
        on
            l.visitor_id = s.visitor_id
            -- AM05: условия соединения в ON
            and s.visit_date <= l.created_at
            -- AM05: фильтр по medium тоже в ON
            and lower(s.medium) in (
                'cpc',
                'cpm',
                'cpa',
                'youtube',
                'cpp',
                'tg',
                'social'
            )
) as t
where t.rn = 1
order by
    t.amount desc nulls last,
    t.visit_date asc,
    t.utm_source asc,
    t.utm_medium asc,
    t.utm_campaign asc
limit 10;
