select x.customer_id,52*((x.sumt/y.countv)*z.visit_per_week)*10 as LTV from 
(
	select customer_id,SUM(total_amount) as sumt
	from [order] 
	group by customer_id
) x
inner join 
(
	select customer_id,count(page_id) as countv
	from site_visit 
	group by customer_id
) y
on x.customer_id=y.customer_id

inner join 
(
	select customer_id, sum(cast(pg_ct as float))/COUNT(cast(wk as float)) as visit_per_week
	from(
		select customer_id,concat(DATEPART (wk, event_time),'-',(year(event_time))) AS wk, count(page_id) as pg_ct from site_visit
		group by customer_id, concat(DATEPART (wk, event_time),'-',(year(event_time))) 
	) w
	group by customer_id
) z
on z.customer_id=y.customer_id