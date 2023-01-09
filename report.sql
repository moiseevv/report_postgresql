
	create temporary table if not exists dogs(id bigint, rform text);
	CREATE index if not exists tmp_dog_id_idx ON dogs USING btree (id);
	truncate dogs;

	insert into dogs
		select d.id, iif(d.form_id in(1), 'ФЛ'::text, 'УК'::text) as rform 
		from sh_billing.dogovor d
		where d.form_id in(1,2);

	-- распределенные платежи
	create temporary table if not exists pays_per(id bigint, rform text, pdocmonth int, pdocyear int, pmonth int, pyear int, ppsumma numeric);
	CREATE INDEX if not exists tmp_pays_per_id_idx ON pays_per USING btree (id);
	truncate pays_per;

	insert into pays_per
		select zz.id, zz.rform, zz.pdocmonth, zz.pdocyear, zz.pmonth, zz.pyear, sum(zz.summa) as ppsumma
		from(
			select distinct
				dogs.id,
				dogs.rform,
				extract(month from maxvalue(coalesce(bp.data_receive, p.docdate), '01.01.2019'::date))::int as pdocmonth,
				extract(year from maxvalue(coalesce(bp.data_receive, p.docdate), '01.01.2019'::date))::int as pdocyear,
				pp.pmonth,
				pp.pyear,
				pp.id as pp_id,
				pp.summa
			from sh_billing.pays p
				join dogs on dogs.id = p.dogovor_id
				join sh_billing.spr_oper_category g on g.id = p.category_id
				join sh_billing.vw_oper_types ot on ot.subtype_id = p.type_id
						and (ot.type_code = 'PAY' or ot.subtype_code = 'NETTING')
				join sh_billing.pays_period pp on pp.pay_id = p.id
					and pp.is_actual
				left join sh_exchange.in_reestrs r on r.id = p.reestr_id and r.is_actual
				left join sh_exchange.bks_pays bp on (bp.id = p.bks_pay_id or bp.id = r.bks_pay_id) and bp.is_actual
			where p.is_actual
				and coalesce(bp.data_receive, p.docdate) >= '01.01.2019'
				and (bp.id is not null or ot.subtype_code in ('NETTING', 'PENI_MOVE_OVER_PAY', 'PAY_RETURN', 'PAY_TO_PENI', 'PAY_TO_GOS', 'CORR_MINUS'))-- or ot.type_code = 'DC_PAY') --'MOVE_PAY',
		) zz
		group by 1,2,3,4,5,6
	;

	create temporary table if not exists pays_total(id bigint, rform text, pdocmonth int, pdocyear int, psumma numeric);
	CREATE INDEX if not exists tmp_pays_total_id_idx ON pays_total USING btree (id);
	truncate pays_total;

	insert into pays_total
			select 
				dogs.id,
				dogs.rform,
				extract(month from maxvalue(coalesce(bp.data_receive, p.docdate), '01.01.2019'::date))::int as pdocmonth,
				extract(year from maxvalue(coalesce(bp.data_receive, p.docdate), '01.01.2019'::date))::int as pdocyear,		
				sum(-p.summa * ot.vsign * g.vsign) as psumma-- pay_remain
			from sh_billing.pays p
				join dogs on dogs.id = p.dogovor_id
				join sh_billing.spr_oper_category g on g.id = p.category_id
				join sh_billing.vw_oper_types ot on ot.subtype_id = p.type_id
						and (ot.type_code = 'PAY' or ot.subtype_code = 'NETTING' or ot.type_code = 'DC_PAY')
				left join sh_exchange.in_reestrs r on r.id = p.reestr_id and r.is_actual
				left join sh_exchange.bks_pays bp on (bp.id = p.bks_pay_id or bp.id = r.bks_pay_id) and bp.is_actual
			where p.is_actual
				and p.docdate >= '01.01.2019'
				and (bp.id is not null or ot.subtype_code in ('NETTING', 'PENI_MOVE_OVER_PAY', 'PAY_RETURN', 'PAY_TO_PENI', 'PAY_TO_GOS', 'CORR_MINUS')) -- or ot.type_code = 'DC_PAY') -- 'MOVE_PAY', 
			group by 1,2,3,4
	;

	create temporary table if not exists pays_rem(id bigint, rform text, pdocmonth int, pdocyear int, pay_remain numeric);
	CREATE INDEX if not exists tmp_pays_rem_id_idx ON pays_rem USING btree (id);
	truncate pays_rem;
				
	insert into pays_rem
		select xx.id, xx.rform, xx.pdocmonth, xx.pdocyear, xx.pay_remain
		from (
			select pt.id, pt.rform, pt.pdocmonth, pt.pdocyear, 
				(pt.psumma - 
					coalesce((select sum(pp.ppsumma) from pays_per pp where pp.pdocmonth = pt.pdocmonth and pp.pdocyear = pt.pdocyear and pp.id = pt.id), 0)) as pay_remain
			from pays_total pt
		) xx
		where pay_remain != 0
	;
	copy(
				select
					pp.id,
					pp.pmonth,
					pp.pyear,
					pp.pdocmonth, 
					pp.pdocyear, 
					pp.rform,
					pp.ppsumma
				from pays_per pp
				where  pp.pmonth= 1 and pp.pyear = 2019
					and (pp.pdocyear = 2022 and pp.pdocmonth = 11 )
				union all		
				select
					pr.id,
					coalesce(extract(month from current_date)::int) as pmonth,
					coalesce(extract(year from current_date)::int) as pyear,
					pr.pdocmonth, 
					pr.pdocyear,
					pr.rform,
					sum(pr.pay_remain) as psumm
				from pays_rem pr
				where (pr.pdocyear = 2020 and pr.pdocmonth=11)
					and pr.pay_remain != 0
				group by 1,2,3,4,5,6
) to '/home/vova/result.csv' encoding 'WIN1251' delimiter E'\t' null ''	

