def generate_fiscal_query():
    from datetime import datetime
    from airflow import models
    date_format = "%Y-%m-%d"
    year = datetime.strptime(models.Variable.get("fiscal_start_date"), date_format).year
    gap = int(
        (
            datetime.strptime(models.Variable.get("fiscal_start_date"), date_format)
            - datetime.strptime(f"{year}-01-01", date_format)
        ).days
    )
    create_fiscal_calendar = f"""
              select *except(months,count_overlap_days,max_days,rank), coalesce(max(case when rank>1 and count_overlap_days=max_days then months else null end) over(partition by years,weeks),months) as months, cast(case when weeks<=50 then ceil(weeks/2) else 26 end as int64) as bi_week
               from
              (select *, max(count_overlap_days) over(partition by years,weeks) as max_days
              from              
              (select *,(case when fiscal_week between 1 and 50 then cast(ceil(fiscal_week/2) as int64) 
#                         when fiscal_week between 51 and 53 then 26 end) as fiscal_bi_week,
                      count(distinct months) over(partition by years,weeks) as rank, count(dates) over (partition by years,months,weeks) count_overlap_days
               from
               (
                select fiscal_date as date, extract(YEAR from calendar_date	) as fiscal_year,extract(quarter from calendar_date	) as fiscal_quarter,
                    extract(Month from calendar_date	) as fiscal_month, (extract(week from calendar_date	)+1) fiscal_week , extract(day from calendar_date	) as fiscal_day, 
                    extract(YEAR from calendar_date	)*100 + (extract(week from calendar_date	)+1) as fiscal_year_week,
                    extract(YEAR from calendar_date	)*100 + (extract(month from calendar_date	)+1) as fiscal_year_month,
                    extract(YEAR from calendar_date	)*100 + (extract(quarter from calendar_date	)+1) as fiscal_year_quarter
              from
              (select calendar_date, lead(calendar_date ,{gap}) over( order by calendar_date ) as fiscal_date
              from   (select calendar_date from unnest(GENERATE_DATE_ARRAY('2010-01-01', '2050-01-01')) as calendar_date)
              )
              where extract(YEAR from calendar_date) >2015
              order by calendar_date )
              )
              )
  """
    models.Variable.set("fiscal_table_query", create_fiscal_calendar)
