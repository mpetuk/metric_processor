/*** APPROACH 1: Outlier detection based on median and median absolute deviation ***/

/*** 1.1 Calculate median and other descriptive stats ****/

drop table if exists public_work_tbls.mp_base_90;
create table public_work_tbls.mp_base_90 as
select style_id
      ,model_year
      ,make
      ,ct
      ,round(avg(case when row_id between ct/2.0 and ct/2.0 + 1 then price end)) as median

from 
        (
        select
              x.style_id,
              model_year,
              make,
              price,
              row_number() over (partition by x.style_id order by price) as row_id,
              x.ct

        from
                (
                        SELECT a.price,
                        v.style_id,
                        v.model_year,
                        dd.division_name as make,
                        count(*) over (partition by v.style_id)  as ct 
                        FROM dw.dealer_vin_price_sale_hist_c a
                        INNER JOIN dw.dealer_c d   
                                ON a.dealer_id = d.dealer_id
                        LEFT OUTER JOIN ods.chrome__vin_c v
                                ON a.vin = v.vin
                        inner join ods.chrome_division_definition dd on v.make_id = dd.division_id

                        WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL and v.style_id is not null
                        AND vin_last_dt > DATE '2017-11-30' 
                        AND a.price > 0 
                ) x 
                       
        )  a
group by style_id, model_year, make, ct
;

select count(*), count(distinct style_id) from public_work_tbls.mp_base_90;
--21801	21801

select case when ct < 6 then 1 else 0 end,
       case when ct < 11 then 1 else 0 end,
       case when ct >= 11 then 1 else 0 end,
       count(*)
from  public_work_tbls.mp_base_90
group by case when ct < 6 then 1 else 0 end,
       case when ct < 11 then 1 else 0 end,
       case when ct >= 11 then 1 else 0 end;
/*
0	1	0	2566
1	1	0	3112
0	0	1	16123
*/


select sum(case when ct < 6 then 1 else 0 end),
       sum(case when ct < 11 then 1 else 0 end),
       sum(case when ct >= 11 then 1 else 0 end)
  
from  public_work_tbls.mp_base_90
;
-- 3112	5678	16123

/*** 1.2 Calculate Median and Mean Absolute Deviation stats ***/

drop table if exists public_work_tbls.mp_MADs_90;
create table public_work_tbls.mp_MADs_90 as
select  style_id
       ,avg(case when row_id between ct/2.0 and ct/2.0 + 1 then med_dev end ) as MAD
       ,avg(med_dev) as MeanAD

from 
(
select x.style_id
      ,x.price
      ,abs(x.price - median) as med_dev
      ,row_number() over (partition by x.style_id order by abs(x.price - median)) as row_id
      ,y.ct 
from
                (
                        SELECT a.price,
                               v.style_id
                        FROM dw.dealer_vin_price_sale_hist_c a
                        INNER JOIN dw.dealer_c d   
                                ON a.dealer_id = d.dealer_id
                        LEFT OUTER JOIN ods.chrome__vin_c v
                                ON a.vin = v.vin
                        WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL and v.style_id is not null
                        AND vin_last_dt > DATE '2017-11-30' 
                        AND a.price > 0 
                ) x 
                inner join public_work_tbls.mp_base_90 y 
                on x.style_id = y.style_id
) z
group by style_id,ct
;

select count(*) from public_work_tbls.mp_MADs_90
;

/*** 1.3 Put it together and calculate 8 variations of average price metric using 8 outlier detection variations:
        1. Standard Formula for Outlier Detection using MAD:
                abs(y - median_y) / 1.4826 * MAD > 3.5 
        
        2. Consistancy Coefficient of 1.4826 is assuming the underlying distribution is normal. It is 1 over z score corresponding to 75% of CDF, or 1/Q(0.75) = 1/0.6745.
           Detect outliers without normality assumption:
                abs(y - median_y) / MAD > 3.5
        
        3. Given the nature of the data, it makes sense to be more conservative on the small outliers than on the big ones. Keep normality assumption: 
               (y - median_y) / 1.4826 * MAD < -2.5 or (y - median_y) / 1.4826 * MAD > 3.5
        
        4. Now without normality assumption: 
               (y - median_y) / MAD < -2.5 or (y - median_y) / MAD > 3.5
        
        5, 6, 7, 8: consider 4 methodologies above and add a variation (per IBM) for situations when  more than 50% of the data have identical values: 
                if MAD = 0 use (1.253314*MeanAD) instead of (1.4826*MAD)
        
        Note: For 3 standard deviations from the mean definition of an outlier, the minimal sample size is 11.
*/
drop table if exists public_work_tbls.mp_avgprice_med_11;
create table public_work_tbls.mp_avgprice_med_11 as
select  a.style_id
       
      /* conservative (3.5) cut off (both sides) with 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 11 or abs(a.price - b.median)<= 5.189 * MAD then a.price end)) as avg_price_med
      ,sum(case when b.ct >= 11 and abs(a.price - b.median) > 5.189 * MAD then 1 else 0 end) as outlier_med
      ,cast(sum(case when b.ct >= 11 and abs(a.price - b.median)> 5.189 * MAD then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct

      /* conservative (3.5) cut off (both sides) without adjustment for normality */
      ,round(avg(case when b.ct < 11 or abs(a.price - b.median)<= 3.5 * MAD then a.price end)) as avg_price_med_nob
      ,sum(case when b.ct >= 11 and abs(a.price - b.median) > 3.5 * MAD then 1 else 0 end) as outlier_med_nob
      ,cast(sum(case when b.ct >= 11 and abs(a.price - b.median) > 3.5 * MAD then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) with 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 11 or (a.price - b.median) between -3.7 * MAD  and 5.189 * MAD then a.price end)) as avg_price_med_dl
      ,sum(case when b.ct >= 11 and ((a.price - b.median) < -3.7 * MAD  or (a.price - b.median) > 5.189 * MAD) then 1 else 0 end) as outlier_med_dl
      ,cast(sum(case when b.ct >= 11 and ((a.price - b.median) < -3.7 * MAD  or (a.price - b.median) > 5.189 * MAD) then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_dl

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) without 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 11 or (a.price - b.median) between -2.5 * MAD and 3.5 * MAD then a.price end)) as avg_price_med_nob_dl
      ,sum(case when b.ct >= 11 and ((a.price - b.median) < -2.5 * MAD or (a.price - b.median) > 3.5 * MAD) then 1 else 0 end) as outlier_med_nob_dl
      ,cast(sum(case when b.ct >= 11 and ((a.price - b.median) < -2.5 * MAD or (a.price - b.median) > 3.5 * MAD) then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob_dl


      ------------ plus IBM logic for MAD = 0 situations ------------

      /* conservative (3.5) cut off (both sides) with IBM logic for MAD=0 and 1.482 adjustment for normality*/
      ,round(avg(case when b.ct < 11 or abs(a.price - b.median)<= case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then a.price end)) as avg_price_med_ibm
      ,sum(case when b.ct >= 11 and abs(a.price - b.median) > case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then 1 else 0 end) as outlier_med_ibm
      ,cast(sum(case when b.ct >= 11 and abs(a.price - b.median) > case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_ibm

      /* conservative (3.5) cut off (both sides) with IBM logic for MAD=0 and without adjustment for normality */
      ,round(avg(case when b.ct < 11 or abs(a.price - b.median)<=  3.5 * case when MAD=0 then MeanAD else  MAD end then a.price end)) as avg_price_med_nob_ibm
      ,sum(case when b.ct >= 11 and abs(a.price - b.median) > 3.5 * case when MAD=0 then MeanAD else  MAD end then 1 else 0 end) as outlier_med_nob_ibm
      ,cast(sum(case when b.ct >= 11 and abs(a.price - b.median) > 3.5 * case when MAD=0 then MeanAD else  MAD end then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob_ibm

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) with IBM logic for MAD=0 and 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 11 or (a.price - b.median) between case when MAD=0 then -3.13 * MeanAD else -3.7 * MAD end  and case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then a.price end)) as avg_price_med_dl_ibm
      ,sum(case when b.ct >= 11 and ((a.price - b.median) < case when MAD=0 then -3.13 * MeanAD else -3.7 * MAD end or (a.price - b.median) > case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end) then 1 else 0 end) as outlier_med_dl_ibm
      ,cast(sum(case when b.ct >= 11 and  ((a.price - b.median) < case when MAD=0 then -3.13 * MeanAD else -3.7 * MAD end or (a.price - b.median) > case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end) then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_dl_ibm

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) with IBM logic for MAD=0 and without 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 11 or (a.price - b.median) between -2.5 * case when MAD=0 then MeanAD else  MAD end and 3.5 * case when MAD=0 then MeanAD else  MAD end then a.price end)) as avg_price_med_nob_dl_ibm
      ,sum(case when b.ct >= 11 and ((a.price - b.median) < -2.5 * case when MAD=0 then MeanAD else  MAD end or (a.price - b.median) > 3.5 * case when MAD=0 then MeanAD else  MAD end) then 1 else 0 end) as outlier_med_nob_dl_ibm
      ,cast(sum(case when b.ct >= 11 and ((a.price - b.median) < -2.5 * case when MAD=0 then MeanAD else  MAD end or (a.price - b.median) > 3.5 * case when MAD=0 then MeanAD else  MAD end) then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob_dl_ibm

from                 (
                        SELECT a.price,
                               v.style_id AS style_id
                        FROM dw.dealer_vin_price_sale_hist_c a
                        INNER JOIN dw.dealer_c d   
                                ON a.dealer_id = d.dealer_id
                        LEFT OUTER JOIN ods.chrome__vin_c v
                                ON a.vin = v.vin
                        WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL and v.style_id is not null
                        AND vin_last_dt > DATE '2017-11-30'   
                        AND a.price > 0 
                ) a inner join public_work_tbls.mp_base_90 b on a.style_id = b.style_id
                    inner join public_work_tbls.mp_MADs_90 c on a.style_id = c.style_id
group by a.style_id, ct
;


drop table if exists public_work_tbls.mp_avgprice_med_6;
create table public_work_tbls.mp_avgprice_med_6 as
select  a.style_id
       
      /* conservative (3.5) cut off (both sides) with 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 6 or abs(a.price - b.median)<= 5.189 * MAD then a.price end)) as avg_price_med
      ,sum(case when b.ct >= 6 and abs(a.price - b.median) > 5.189 * MAD then 1 else 0 end) as outlier_med
      ,cast(sum(case when b.ct >= 6 and abs(a.price - b.median)> 5.189 * MAD then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct

      /* conservative (3.5) cut off (both sides) without adjustment for normality */
      ,round(avg(case when b.ct < 6 or abs(a.price - b.median)<= 3.5 * MAD then a.price end)) as avg_price_med_nob
      ,sum(case when b.ct >= 6 and abs(a.price - b.median) > 3.5 * MAD then 1 else 0 end) as outlier_med_nob
      ,cast(sum(case when b.ct >= 6 and abs(a.price - b.median) > 3.5 * MAD then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) with 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 6 or (a.price - b.median) between -3.7 * MAD  and 5.189 * MAD then a.price end)) as avg_price_med_dl
      ,sum(case when b.ct >= 6 and ((a.price - b.median) < -3.7 * MAD  or (a.price - b.median) > 5.189 * MAD) then 1 else 0 end) as outlier_med_dl
      ,cast(sum(case when b.ct >= 6 and ((a.price - b.median) < -3.7 * MAD  or (a.price - b.median) > 5.189 * MAD) then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_dl

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) without 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 6 or (a.price - b.median) between -2.5 * MAD and 3.5 * MAD then a.price end)) as avg_price_med_nob_dl
      ,sum(case when b.ct >= 6 and ((a.price - b.median) < -2.5 * MAD or (a.price - b.median) > 3.5 * MAD) then 1 else 0 end) as outlier_med_nob_dl
      ,cast(sum(case when b.ct >= 6 and ((a.price - b.median) < -2.5 * MAD or (a.price - b.median) > 3.5 * MAD) then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob_dl


      ------------ plus IBM logic for MAD = 0 situations ------------

      /* conservative (3.5) cut off (both sides) with IBM logic for MAD=0 and 1.482 adjustment for normality*/
      ,round(avg(case when b.ct < 6 or abs(a.price - b.median)<= case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then a.price end)) as avg_price_med_ibm
      ,sum(case when b.ct >= 6 and abs(a.price - b.median) > case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then 1 else 0 end) as outlier_med_ibm
      ,cast(sum(case when b.ct >= 6 and abs(a.price - b.median) > case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_ibm

      /* conservative (3.5) cut off (both sides) with IBM logic for MAD=0 and without adjustment for normality */
      ,round(avg(case when b.ct < 6 or abs(a.price - b.median)<=  3.5 * case when MAD=0 then MeanAD else  MAD end then a.price end)) as avg_price_med_nob_ibm
      ,sum(case when b.ct >= 6 and abs(a.price - b.median) > 3.5 * case when MAD=0 then MeanAD else  MAD end then 1 else 0 end) as outlier_med_nob_ibm
      ,cast(sum(case when b.ct >= 6 and abs(a.price - b.median) > 3.5 * case when MAD=0 then MeanAD else  MAD end then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob_ibm

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) with IBM logic for MAD=0 and 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 6 or (a.price - b.median) between case when MAD=0 then -3.13 * MeanAD else -3.7 * MAD end  and case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then a.price end)) as avg_price_med_dl_ibm
      ,sum(case when b.ct >= 6 and ((a.price - b.median) < case when MAD=0 then -3.13 * MeanAD else -3.7 * MAD end or (a.price - b.median) > case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end) then 1 else 0 end) as outlier_med_dl_ibm
      ,cast(sum(case when b.ct >= 6 and  ((a.price - b.median) < case when MAD=0 then -3.13 * MeanAD else -3.7 * MAD end or (a.price - b.median) > case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end) then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_dl_ibm

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) with IBM logic for MAD=0 and without 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 6 or (a.price - b.median) between -2.5 * case when MAD=0 then MeanAD else  MAD end and 3.5 * case when MAD=0 then MeanAD else  MAD end then a.price end)) as avg_price_med_nob_dl_ibm
      ,sum(case when b.ct >= 11 and ((a.price - b.median) < -2.5 * case when MAD=0 then MeanAD else  MAD end or (a.price - b.median) > 3.5 * case when MAD=0 then MeanAD else  MAD end) then 1 else 0 end) as outlier_med_nob_dl_ibm
      ,cast(sum(case when b.ct >= 11 and ((a.price - b.median) < -2.5 * case when MAD=0 then MeanAD else  MAD end or (a.price - b.median) > 3.5 * case when MAD=0 then MeanAD else  MAD end) then 1 else 0 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob_dl_ibm

from                 (
                        SELECT a.price,
                               v.style_id AS style_id
                        FROM dw.dealer_vin_price_sale_hist_c a
                        INNER JOIN dw.dealer_c d   
                                ON a.dealer_id = d.dealer_id
                        LEFT OUTER JOIN ods.chrome__vin_c v
                                ON a.vin = v.vin
                        WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL and v.style_id is not null
                        AND vin_last_dt > DATE '2017-11-30'   
                        AND a.price > 0 
                ) a inner join public_work_tbls.mp_base_90 b on a.style_id = b.style_id
                    inner join public_work_tbls.mp_MADs_90 c on a.style_id = c.style_id
group by a.style_id, ct
;

/* APPROACH 2: Define outliers as top 1% and bottom 1% centile of the distribution.
Not a great option but distribution agnostic. */

drop table if exists public_work_tbls.mp_avgprice_ntiles_11;
create table public_work_tbls.mp_avgprice_ntiles_11 as
select x.style_id
      ,round(avg(case when b.ct < 11 or centile between 2 and 99 then price end)) as avg_price_ntiles
      ,sum(case when b.ct >= 11 and (centile < 2 or centile > 99) then 1 else 0 end) as outlier_ntiles
      ,cast(sum(case when b.ct >= 11 and (centile < 2 or centile > 99) then 1 else 0 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_ntiles_pct

from
        (
        select style_id
              ,price
              ,ntile(100) over (partition by style_id order by price desc) as centile  
        from
                (
                SELECT a.price
                      ,v.style_id
                FROM dw.dealer_vin_price_sale_hist_c a
                INNER JOIN dw.dealer_c d   
                        ON a.dealer_id = d.dealer_id
                LEFT OUTER JOIN ods.chrome__vin_c v
                        ON a.vin = v.vin
                WHERE -- sold_flg AND 
                        latest_record_flg
                        AND v.model_year IS NOT NULL and v.style_id is not null
                        AND vin_last_dt > DATE '2017-11-30' 
                        AND a.price > 0
                ) c       
        ) x inner join public_work_tbls.mp_base_90 b on x.style_id = b.style_id
group by x.style_id
;

drop table if exists public_work_tbls.mp_avgprice_ntiles_6;
create table public_work_tbls.mp_avgprice_ntiles_6 as
select x.style_id
      ,round(avg(case when b.ct < 6 or centile between 2 and 99 then price end)) as avg_price_ntiles
      ,sum(case when b.ct >= 6 and (centile < 2 or centile > 99) then 1 else 0 end) as outlier_ntiles
      ,cast(sum(case when b.ct >= 6 and (centile < 2 or centile > 99) then 1 else 0 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_ntiles_pct

from
        (
        select style_id
              ,price
              ,ntile(100) over (partition by style_id order by price desc) as centile  
        from
                (
                SELECT a.price
                      ,v.style_id
                FROM dw.dealer_vin_price_sale_hist_c a
                INNER JOIN dw.dealer_c d   
                        ON a.dealer_id = d.dealer_id
                LEFT OUTER JOIN ods.chrome__vin_c v
                        ON a.vin = v.vin
                WHERE -- sold_flg AND 
                        latest_record_flg
                        AND v.model_year IS NOT NULL and v.style_id is not null
                        AND vin_last_dt > DATE '2017-11-30' 
                        AND a.price > 0
                ) c       
        ) x inner join public_work_tbls.mp_base_90 b on x.style_id = b.style_id
group by x.style_id
;

/* APPROACH 3: Using the Empirical Rule and Chebyshev's Theorem to define outliers:
       The Empirical Rule. One of the commonest ways of finding outliers. Assumptions: Normal, bell-shaped distribution
                ~68% lie within 1 std from the mean
                ~95% lie within 2 std from the mean
                ~99.7% within 3 std from the mean
        Chebyshev's Theorem. Any distribution
                at least 75% within 2 std from the mean
                at least 88.9% within 3 std from the mean
                at least 96% within 5 std from the mean 
*/
drop table if exists public_work_tbls.mp_avgprice_3std_11;
create table public_work_tbls.mp_avgprice_3std_11 as                  
select  x.style_id
    
       ---- The Empirical Rule. Using 3 standard deviations from sample mean ----
       ,round(avg(case when b.ct < 11 or price between (avg_price - 3*x.std) and (avg_price + 3*x.std) then price end)) as avg_price_emp 
       ,sum(case when b.ct >= 11 and (price < (avg_price - 3*x.std) or price > (avg_price + 3*x.std)) then 1 else 0 end) as outlier_emp
       ,cast (sum(case when b.ct >= 11 and (price < (avg_price - 3*x.std) or price > (avg_price + 3*x.std)) then 1 else 0 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_emp_pct

       ---- Chebyshev's Theorem. Using 4.5 standard deviations distance from sample mean ----
       ,round(avg(case when b.ct < 11 or price between (avg_price - 4.5*x.std) and (avg_price + 4.5*x.std) then price end)) as avg_price_chb
       ,sum(case when b.ct >= 11 and (price < (avg_price - 4.5*x.std) or price > (avg_price + 4.5*x.std)) then 1 else 0 end) as outlier_chb
       ,cast (sum(case when b.ct >= 11 and (price < (avg_price - 4.5*x.std) or price > (avg_price + 4.5*x.std)) then 1 else 0 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_chb_pct

from
        (                  
        select style_id
              ,price 
              ,avg(price) over (partition by style_id order by null) as avg_price    
              ,stddev(price)  over (partition by style_id order by null) as  std
        from 
                (
                 SELECT a.price,
                        v.style_id 
                FROM dw.dealer_vin_price_sale_hist_c a
                INNER JOIN dw.dealer_c d   
                        ON a.dealer_id = d.dealer_id
                LEFT OUTER JOIN ods.chrome__vin_c v
                        ON a.vin = v.vin
                WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL and v.style_id is not null
                        AND vin_last_dt > DATE '2017-11-30'  
                        AND a.price > 0
                ) c     
        ) x inner join public_work_tbls.mp_base_90 b on x.style_id = b.style_id
 group by x.style_id
 ;  



drop table if exists public_work_tbls.mp_avgprice_3std_6;
create table public_work_tbls.mp_avgprice_3std_6 as                  
select  x.style_id
    
       ---- The Empirical Rule. Using 3 standard deviations from sample mean ----
       ,round(avg(case when b.ct < 6 or price between (avg_price - 3*x.std) and (avg_price + 3*x.std) then price end)) as avg_price_emp 
       ,sum(case when b.ct >= 6 and (price < (avg_price - 3*x.std) or price > (avg_price + 3*x.std)) then 1 else 0 end) as outlier_emp
       ,cast (sum(case when b.ct >= 6 and (price < (avg_price - 3*x.std) or price > (avg_price + 3*x.std)) then 1 else 0 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_emp_pct

       ---- Chebyshev's Theorem. Using 4.5 stadard deviations distance from sample mean ----
       ,round(avg(case when b.ct < 6 or price between (avg_price - 4.5*x.std) and (avg_price + 4.5*x.std) then price end)) as avg_price_chb
       ,sum(case when b.ct >= 6 and (price < (avg_price - 4.5*x.std) or price > (avg_price + 4.5*x.std)) then 1 else 0 end) as outlier_chb
       ,cast (sum(case when b.ct >= 6 and (price < (avg_price - 4.5*x.std) or price > (avg_price + 4.5*x.std)) then 1 else 0 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_chb_pct
from
        (                  
        select style_id
              ,price 
              ,avg(price) over (partition by style_id order by null) as avg_price    
              ,stddev(price)  over (partition by style_id order by null) as  std
        from 
                (
                 SELECT a.price,
                        v.style_id 
                FROM dw.dealer_vin_price_sale_hist_c a
                INNER JOIN dw.dealer_c d   
                        ON a.dealer_id = d.dealer_id
                LEFT OUTER JOIN ods.chrome__vin_c v
                        ON a.vin = v.vin
                WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL and v.style_id is not null
                        AND vin_last_dt > DATE '2017-11-30' 
                        AND a.price > 0
                ) c     
        ) x inner join public_work_tbls.mp_base_90 b on x.style_id = b.style_id
 group by x.style_id
 ;  

/* Approach 4: Using Empirical Rule's 2 and 3 standard deviations distance on log-transformed price (for normalization of distribution) */

drop table if exists public_work_tbls.mp_avgprice_3std_log_11;
create table public_work_tbls.mp_avgprice_3std_log_11 as                  
select  x.style_id
       ,sum(case when b.ct >= 11 and (ln(price) < (avg_logprice - 3*x.std) or ln(price) > (avg_logprice + 3*x.std)) then 1 else 0 end) as outlier_3std
       ,cast (sum(case when b.ct >= 11 and (ln(price) < (avg_logprice - 3*x.std) or ln(price) > (avg_logprice + 3*x.std)) then 1 else 0 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_3std_pct
       ,round(avg(case when b.ct < 11  or ln(price) between (avg_logprice - 3*x.std) and (avg_logprice + 3*x.std) then price end)) as avg_price_3std
       
       ,sum(case when b.ct >= 11 and (ln(price) < (avg_logprice - 2*x.std) or ln(price) > (avg_logprice + 2*x.std)) then 1 else 0 end) as outlier_2std
       ,cast (sum(case when b.ct >= 11 and (ln(price) < (avg_logprice - 2*x.std) or ln(price) > (avg_logprice + 2*x.std)) then 1 else 0 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_2std_pct
       ,round(avg(case when b.ct < 11 or ln(price) between (avg_logprice - 2*x.std) and (avg_logprice + 2*x.std) then price end)) as avg_price_2std
from
        (                  
        select style_id
              ,price
              ,avg(ln(price)) over (partition by style_id order by null) as avg_logprice    
              ,stddev(ln(price)) over (partition by style_id order by null) as  std
        from 
                (
                 SELECT a.price,
                        v.style_id
                FROM dw.dealer_vin_price_sale_hist_c a
                INNER JOIN dw.dealer_c d   
                        ON a.dealer_id = d.dealer_id
                LEFT OUTER JOIN ods.chrome__vin_c v
                        ON a.vin = v.vin
                WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL and v.style_id is not null
                        AND vin_last_dt > DATE '2017-11-30'  
                        AND a.price > 0
                ) c     
        ) x inner join public_work_tbls.mp_base_90 b on x.style_id = b.style_id
 group by x.style_id
 ;  


drop table if exists public_work_tbls.mp_avgprice_3std_log_6;
create table public_work_tbls.mp_avgprice_3std_log_6 as                  
select  x.style_id
       ,sum(case when b.ct >= 6 and (ln(price) < (avg_logprice - 3*x.std) or ln(price) > (avg_logprice + 3*x.std)) then 1 else 0 end) as outlier_3std
       ,cast (sum(case when b.ct >= 6 and (ln(price) < (avg_logprice - 3*x.std) or ln(price) > (avg_logprice + 3*x.std)) then 1 else 0 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_3std_pct
       ,round(avg(case when b.ct < 6  or ln(price) between (avg_logprice - 3*x.std) and (avg_logprice + 3*x.std) then price end)) as avg_price_3std
       
       ,sum(case when b.ct >= 6 and (ln(price) < (avg_logprice - 2*x.std) or ln(price) > (avg_logprice + 2*x.std)) then 1 else 0 end) as outlier_2std
       ,cast (sum(case when b.ct >= 6 and (ln(price) < (avg_logprice - 2*x.std) or ln(price) > (avg_logprice + 2*x.std)) then 1 else 0 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_2std_pct
       ,round(avg(case when b.ct < 6 or ln(price) between (avg_logprice - 2*x.std) and (avg_logprice + 2*x.std) then price end)) as avg_price_2std
from
        (                  
        select style_id
              ,price
              ,avg(ln(price)) over (partition by style_id order by null) as avg_logprice    
              ,stddev(ln(price)) over (partition by style_id order by null) as  std
        from 
                (
                 SELECT a.price,
                        v.style_id
                FROM dw.dealer_vin_price_sale_hist_c a
                INNER JOIN dw.dealer_c d   
                        ON a.dealer_id = d.dealer_id
                LEFT OUTER JOIN ods.chrome__vin_c v
                        ON a.vin = v.vin
                WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL and v.style_id is not null
                        AND vin_last_dt > DATE '2017-11-30'  
                        AND a.price > 0
                ) c     
        ) x inner join public_work_tbls.mp_base_90 b on x.style_id = b.style_id
 group by x.style_id
 ;  


/*** Put all metrics together ****/

drop table if exists public_work_tbls.mp_avgprice_metrics_90_2;
create table public_work_tbls.mp_avgprice_metrics_90_2 as
select distinct 
       a.style_id
      ,x.model_year
      ,x.make 
      ,x.ct as cnt_90day

      ,x.median as median
      ,xx.MAD
      ,xx.MeanAD
      
      ,d.avg_price_med as avg_price_med_11
      ,d.outlier_med as outlier_med_11
      ,d.outlier_med_pct as outlier_med_pct_11
      
      ,d.avg_price_med_nob as avg_price_med_nob_11
      ,d.outlier_med_nob as outlier_med_nob_11
      ,d.outlier_med_pct_nob as outlier_med_pct_nob_11   
 
      ,d.avg_price_med_dl as avg_price_med_dl_11
      ,d.outlier_med_dl as outlier_med_dl_11
      ,d.outlier_med_pct_dl as outlier_med_pct_dl_11 
      
      ,d.avg_price_med_nob_dl as avg_price_med_nob_dl_11
      ,d.outlier_med_nob_dl as outlier_med_nob_dl_11
      ,d.outlier_med_pct_nob_dl as outlier_med_pct_nob_dl_11       
           
           
      ,d.avg_price_med_ibm as avg_price_med_ibm_11
      ,d.outlier_med_ibm as outlier_med_ibm_11
      ,d.outlier_med_pct_ibm as outlier_med_pct_ibm_11
      
      ,d.avg_price_med_nob_ibm as avg_price_med_nob_ibm_11
      ,d.outlier_med_nob_ibm as outlier_med_nob_ibm_11
      ,d.outlier_med_pct_nob_ibm as outlier_med_pct_nob_ibm_11  
 
      ,d.avg_price_med_dl_ibm as avg_price_med_dl_ibm_11
      ,d.outlier_med_dl_ibm as outlier_med_dl_ibm_11
      ,d.outlier_med_pct_dl_ibm as outlier_med_pct_dl_ibm_11  
      
      ,d.avg_price_med_nob_dl_ibm as avg_price_med_nob_dl_ibm_11
      ,d.outlier_med_nob_dl_ibm as outlier_med_nob_dl_ibm_11
      ,d.outlier_med_pct_nob_dl_ibm as outlier_med_pct_nob_dl_ibm_11          
      
     
      
      ,a.avg_price_ntiles as avg_price_ntiles_11
      ,a.outlier_ntiles as outlier_ntiles_11
      ,a.outlier_ntiles_pct as outlier_ntiles_pct_11

      ,b.avg_price_emp as avg_price_emp_11
      ,b.outlier_emp as outlier_emp_11
      ,b.outlier_emp_pct as outlier_emp_pct_11
      
      ,b.avg_price_chb as avg_price_chb_11
      ,b.outlier_chb as outlier_chb_11
      ,b.outlier_chb_pct as outlier_chb_pct_11

      ,c.avg_price_3std as avg_price_3std_log_11
      ,c.outlier_3std as outlier_3std_log_11
      ,c.outlier_3std_pct as outlier_3std_pct_log_11

      ,c.avg_price_2std as avg_price_2std_log_11
      ,c.outlier_2std as outlier_2std_log_11
      ,c.outlier_2std_pct as outlier_2std_pct_log_11

      --- 6 days cut ---
      
      ,d2.avg_price_med as avg_price_med_6
      ,d2.outlier_med as outlier_med_6
      ,d2.outlier_med_pct as outlier_med_pct_6
      
      ,d2.avg_price_med_nob as avg_price_med_nob_6
      ,d2.outlier_med_nob as outlier_med_nob_6
      ,d2.outlier_med_pct_nob as outlier_med_pct_nob_6   
 
      ,d2.avg_price_med_dl as avg_price_med_dl_6
      ,d2.outlier_med_dl as outlier_med_dl_6
      ,d2.outlier_med_pct_dl as outlier_med_pct_dl_6  
      
      ,d2.avg_price_med_nob_dl as avg_price_med_nob_dl_6
      ,d2.outlier_med_nob_dl as outlier_med_nob_dl_6
      ,d2.outlier_med_pct_nob_dl as outlier_med_pct_nob_dl_6       
           
           
      ,d2.avg_price_med_ibm as avg_price_med_ibm_6
      ,d2.outlier_med_ibm as outlier_med_ibm_6
      ,d2.outlier_med_pct_ibm as outlier_med_pct_ibm_6
      
      ,d2.avg_price_med_nob_ibm as avg_price_med_nob_ibm_6
      ,d2.outlier_med_nob_ibm as outlier_med_nob_ibm_6
      ,d2.outlier_med_pct_nob_ibm as outlier_med_pct_nob_ibm_6  
 
      ,d2.avg_price_med_dl_ibm as avg_price_med_dl_ibm_6
      ,d2.outlier_med_dl_ibm as outlier_med_dl_ibm_6
      ,d2.outlier_med_pct_dl_ibm as outlier_med_pct_dl_ibm_6  
      
      ,d2.avg_price_med_nob_dl_ibm as avg_price_med_nob_dl_ibm_6
      ,d2.outlier_med_nob_dl_ibm as outlier_med_nob_dl_ibm_6
      ,d2.outlier_med_pct_nob_dl_ibm as outlier_med_pct_nob_dl_ibm_6          
      
     
      
      ,a2.avg_price_ntiles as avg_price_ntiles_6
      ,a2.outlier_ntiles as outlier_ntiles_6
      ,a2.outlier_ntiles_pct as outlier_ntiles_pct_6

      ,b2.avg_price_emp as avg_price_emp_6
      ,b2.outlier_emp as outlier_emp_6
      ,b2.outlier_emp_pct as outlier_emp_pct_6
      
      ,b2.avg_price_chb as avg_price_chb_6
      ,b2.outlier_chb as outlier_chb_6
      ,b2.outlier_chb_pct as outlier_chb_pct_6

      ,c2.avg_price_3std as avg_price_3std_log_6
      ,c2.outlier_3std as outlier_3std_log_6
      ,c2.outlier_3std_pct as outlier_3std_pct_log_6

      ,c2.avg_price_2std as avg_price_2std_log_6
      ,c2.outlier_2std as outlier_2std_log_6
      ,c2.outlier_2std_pct as outlier_2std_pct_log_6


from public_work_tbls.mp_avgprice_ntiles_11 a inner join public_work_tbls.mp_avgprice_3std_11 b on a.style_id=b.style_id
                                              inner join public_work_tbls.mp_avgprice_3std_log_11 c on a.style_id=c.style_id
                                              inner join public_work_tbls.mp_avgprice_med_11 d on a.style_id=d.style_id
                                              
                                              inner join public_work_tbls.mp_avgprice_ntiles_6 a2 on a.style_id=a2.style_id
                                              inner join public_work_tbls.mp_avgprice_3std_6 b2 on a.style_id=b2.style_id
                                              inner join public_work_tbls.mp_avgprice_3std_log_6 c2 on a.style_id=c2.style_id
                                              inner join public_work_tbls.mp_avgprice_med_6 d2 on a.style_id=d2.style_id
                                              inner join public_work_tbls.mp_base_90 x on a.style_id=x.style_id
                                              inner join public_work_tbls.mp_MADs_90 xx on a.style_id = xx.style_id

                              
;



/**** Calculate Mean Absolute Error for each of 14 methods for comparison 
       MAE = sum (abs(price - price_hat))/ n
       
****/
drop table if exists public_work_tbls.mp_choose_one;
create table public_work_tbls.mp_choose_one as

select 
--avg(a.price) as price_rough
cast(sum(abs(case when b.ct < 11 or abs(b.price - a.median)<= 5.189 * MAD then b.price end - avg_price_med_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_1
,cast(sum(abs(case when b.ct < 11 or abs(b.price - a.median)<= 3.5 * MAD then b.price end - avg_price_med_nob_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_2
,cast(sum(abs(case when b.ct < 11 or (b.price - a.median) between -3.7 * MAD  and 5.189 * MAD then b.price end - avg_price_med_dl_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_3
,cast(sum(abs(case when b.ct < 11 or (b.price - a.median) between -2.5 * MAD and 3.5 * MAD then b.price end - avg_price_med_nob_dl_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_4
,cast(sum(abs(case when b.ct < 11 or abs(b.price - a.median)<= case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then b.price end - avg_price_med_ibm_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_5
,cast(sum(abs(case when b.ct < 11 or abs(b.price - a.median)<=  3.5 * case when MAD=0 then MeanAD else  MAD end then b.price end - avg_price_med_nob_ibm_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_6
,cast(sum(abs(case when b.ct < 11 or (b.price - a.median) between case when MAD=0 then -3.13 * MeanAD else -3.7 * MAD end  and case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then b.price end - avg_price_med_dl_ibm_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_7
,cast(sum(abs(case when b.ct < 11 or (b.price - a.median) between -2.5 * case when MAD=0 then MeanAD else  MAD end and 3.5 * case when MAD=0 then MeanAD else  MAD end then b.price end - avg_price_med_nob_dl_ibm_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_8
,cast(sum(abs(case when b.ct < 11 or centile between 2 and 99 then price end - avg_price_ntiles_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_9
,cast(sum(abs(case when b.ct < 11 or price between (avg_price - 3*std) and (avg_price + 3*std) then price end - avg_price_emp_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_10
,cast(sum(abs(case when b.ct < 11 or price between (avg_price - 4.5*std) and (avg_price + 4.5*std) then price end - avg_price_chb_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_11
,cast(sum(abs(case when b.ct < 11  or ln(price) between (avg_logprice - 3*std_logprice) and (avg_logprice + 3*std_logprice) then price end - avg_price_3std_log_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_12
,cast(sum(abs(case when b.ct < 11 or ln(price) between (avg_logprice - 2*std_logprice) and (avg_logprice + 2*std_logprice) then price end - avg_price_2std_log_11)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_13

,cast(sum(abs(case when b.ct < 6 or abs(b.price - a.median)<= 5.189 * MAD then b.price end - avg_price_med_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_14
,cast(sum(abs(case when b.ct < 6 or abs(b.price - a.median)<= 3.5 * MAD then b.price end - avg_price_med_nob_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_15
,cast(sum(abs(case when b.ct < 6 or (b.price - a.median) between -3.7 * MAD  and 5.189 * MAD then b.price end - avg_price_med_dl_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_16
,cast(sum(abs(case when b.ct < 6 or (b.price - a.median) between -2.5 * MAD and 3.5 * MAD then b.price end - avg_price_med_nob_dl_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_17
,cast(sum(abs(case when b.ct < 6 or abs(b.price - a.median)<= case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then b.price end - avg_price_med_ibm_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_18
,cast(sum(abs(case when b.ct < 6 or abs(b.price - a.median)<=  3.5 * case when MAD=0 then MeanAD else  MAD end then b.price end - avg_price_med_nob_ibm_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_19
,cast(sum(abs(case when b.ct < 6 or (b.price - a.median) between case when MAD=0 then -3.13 * MeanAD else -3.7 * MAD end  and case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then b.price end - avg_price_med_dl_ibm_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_20
,cast(sum(abs(case when b.ct < 6 or (b.price - a.median) between -2.5 * case when MAD=0 then MeanAD else  MAD end and 3.5 * case when MAD=0 then MeanAD else  MAD end then b.price end - avg_price_med_nob_dl_ibm_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_21
,cast(sum(abs(case when b.ct < 6 or b.centile between 2 and 99 then price end - avg_price_ntiles_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_22
,cast(sum(abs(case when b.ct < 6 or price between (b.avg_price - 3*std) and (avg_price + 3*std) then price end - avg_price_emp_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_23
,cast(sum(abs(case when b.ct < 6 or price between (b.avg_price - 4.5*std) and (avg_price + 4.5*std) then price end - avg_price_chb_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_24
,cast(sum(abs(case when b.ct < 6  or ln(price) between (b.avg_logprice - 3*std_logprice) and (avg_logprice + 3*std_logprice) then price end - avg_price_3std_log_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_25
,cast(sum(abs(case when b.ct < 6 or ln(price) between (b.avg_logprice - 2*std_logprice) and (avg_logprice + 2*std_logprice) then price end - avg_price_2std_log_6)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_26

from (
                        SELECT a.price
                               ,v.style_id
                               ,count(*) over (partition by v.style_id)  as ct
                               ,ntile(100) over (partition by style_id order by price desc) as centile
                               ,avg(price) over (partition by style_id order by null) as avg_price    
                               ,stddev(price)  over (partition by style_id order by null) as  std
                               ,avg(ln(price)) over (partition by style_id order by null) as avg_logprice    
                               ,stddev(ln(price)) over (partition by style_id order by null) as  std_logprice
                        FROM dw.dealer_vin_price_sale_hist_c a
                        INNER JOIN dw.dealer_c d   
                                ON a.dealer_id = d.dealer_id
                        LEFT OUTER JOIN ods.chrome__vin_c v
                                ON a.vin = v.vin
                        WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL and v.style_id is not null
                        AND vin_last_dt > DATE '2017-11-30' 
                        AND a.price > 0 
                ) b inner join public_work_tbls.mp_avgprice_metrics_90_2 a on b.style_id = a.style_id


;

select * from public_work_tbls.mp_choose_one;


select 
model_year,
sum(case when cnt_90day < 6 then 1 else 0 end) as num_of_styleid_with_lt_6_cars,
sum(case when cnt_90day < 11 then 1 else 0 end) as num_of_styleid_with_lt_11_cars,
sum(case when cnt_90day >=11 then 1 else 0 end) as num_of_styleid_with_ge_11_cars,
count(*) as total_styleid,
sum(cnt_90day) as total_cars,

sum(case when coalesce(outlier_med_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_11,
sum(case when coalesce(outlier_med_nob_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_11,
sum(case when coalesce(outlier_med_dl_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_dl_11,
sum(case when coalesce(outlier_med_nob_dl_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_dl_11,
sum(case when coalesce(outlier_med_ibm_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_ibm_11,
sum(case when coalesce(outlier_med_nob_ibm_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_ibm_11,
sum(case when coalesce(outlier_med_dl_ibm_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_dl_ibm_11,
sum(case when coalesce(outlier_med_nob_dl_ibm_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_dl_ibm_11,
sum(case when coalesce(outlier_ntiles_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_ntiles_11,
sum(case when coalesce(outlier_emp_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_emp_11,
sum(case when coalesce(outlier_chb_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_chb_11,
sum(case when coalesce(outlier_3std_log_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_3std_log_11,
sum(case when coalesce(outlier_2std_log_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_2std_log_11,

sum(outlier_med_11) as outlier_med_11,
sum(outlier_med_nob_11) as outlier_med_nob_11,
sum(outlier_med_dl_11) as outlier_med_dl_11,
sum(outlier_med_nob_dl_11) as outlier_med_nob_dl_11,
sum(outlier_med_ibm_11) as outlier_med_ibm_11,
sum(outlier_med_nob_ibm_11) as outlier_med_nob_ibm_11,
sum(outlier_med_dl_ibm_11) as outlier_med_dl_ibm_11,
sum(outlier_med_nob_dl_ibm_11) as outlier_med_nob_dl_ibm_11,
sum(outlier_ntiles_11) as outlier_ntiles_11,
sum(outlier_emp_11) as outlier_emp_11,
sum(outlier_chb_11) as outlier_chb_11,
sum(outlier_3std_log_11) as outlier_3std_log_11,
sum(outlier_2std_log_11) as outlier_2std_log_11,

sum(cnt_90day - outlier_med_11) as cnt_med_11,
sum(cnt_90day - outlier_med_nob_11) as cnt_med_nob_11,
sum(cnt_90day - outlier_med_dl_11) as cnt_med_dl_11,
sum(cnt_90day - outlier_med_nob_dl_11) as cnt_med_nob_dl_11,
sum(cnt_90day - outlier_med_ibm_11) as cnt_med_ibm_11,
sum(cnt_90day - outlier_med_nob_ibm_11) as cnt_med_nob_ibm_11,
sum(cnt_90day - outlier_med_dl_ibm_11) as cnt_med_dl_ibm_11,
sum(cnt_90day - outlier_med_nob_dl_ibm_11) as cnt_med_nob_dl_ibm_11,
sum(cnt_90day - outlier_ntiles_11) as cnt_ntiles_11,
sum(cnt_90day - outlier_emp_11) as cnt_emp_11,
sum(cnt_90day - outlier_chb_11) as cnt_chb_11,
sum(cnt_90day - outlier_3std_log_11) as cnt_3std_log_11,
sum(cnt_90day - outlier_2std_log_11) as cnt_2std_log_11,

avg(avg_price_med_11) as avg_price_med_11,
avg(avg_price_med_nob_11) as avg_price_med_nob_11,
avg(avg_price_med_dl_11) as avg_price_med_dl_11,
avg(avg_price_med_nob_dl_11) as avg_price_med_nob_dl_11,
avg(avg_price_med_ibm_11) as avg_price_med_ibm_11,
avg(avg_price_med_nob_ibm_11) as avg_price_med_nob_ibm_11,
avg(avg_price_med_dl_ibm_11) as avg_price_med_dl_ibm_11,
avg(avg_price_med_nob_dl_ibm_11) as avg_price_med_nob_dl_ibm_11,
avg(avg_price_ntiles_11) as avg_price_ntiles_11,
avg(avg_price_emp_11) as avg_price_emp_11,
avg(avg_price_chb_11) as avg_price_chb_11,
avg(avg_price_3std_log_11) as avg_price_3std_log_11,
avg(avg_price_2std_log_11) as avg_price_2std_log_11

from public_work_tbls.mp_avgprice_metrics_90_2
group by model_year
;

select 
make,
sum(case when cnt_90day < 6 then 1 else 0 end) as num_of_styleid_with_lt_6_cars,
sum(case when cnt_90day < 11 then 1 else 0 end) as num_of_styleid_with_lt_11_cars,
sum(case when cnt_90day >=11 then 1 else 0 end) as num_of_styleid_with_ge_11_cars,
count(*) as total_styleid,
sum(cnt_90day) as total_cars,

sum(case when coalesce(outlier_med_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_11,
sum(case when coalesce(outlier_med_nob_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_11,
sum(case when coalesce(outlier_med_dl_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_dl_11,
sum(case when coalesce(outlier_med_nob_dl_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_dl_11,
sum(case when coalesce(outlier_med_ibm_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_ibm_11,
sum(case when coalesce(outlier_med_nob_ibm_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_ibm_11,
sum(case when coalesce(outlier_med_dl_ibm_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_dl_ibm_11,
sum(case when coalesce(outlier_med_nob_dl_ibm_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_dl_ibm_11,
sum(case when coalesce(outlier_ntiles_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_ntiles_11,
sum(case when coalesce(outlier_emp_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_emp_11,
sum(case when coalesce(outlier_chb_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_chb_11,
sum(case when coalesce(outlier_3std_log_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_3std_log_11,
sum(case when coalesce(outlier_2std_log_11,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_2std_log_11,

sum(outlier_med_11) as outlier_med_11,
sum(outlier_med_nob_11) as outlier_med_nob_11,
sum(outlier_med_dl_11) as outlier_med_dl_11,
sum(outlier_med_nob_dl_11) as outlier_med_nob_dl_11,
sum(outlier_med_ibm_11) as outlier_med_ibm_11,
sum(outlier_med_nob_ibm_11) as outlier_med_nob_ibm_11,
sum(outlier_med_dl_ibm_11) as outlier_med_dl_ibm_11,
sum(outlier_med_nob_dl_ibm_11) as outlier_med_nob_dl_ibm_11,
sum(outlier_ntiles_11) as outlier_ntiles_11,
sum(outlier_emp_11) as outlier_emp_11,
sum(outlier_chb_11) as outlier_chb_11,
sum(outlier_3std_log_11) as outlier_3std_log_11,
sum(outlier_2std_log_11) as outlier_2std_log_11,

sum(cnt_90day - outlier_med_11) as cnt_med_11,
sum(cnt_90day - outlier_med_nob_11) as cnt_med_nob_11,
sum(cnt_90day - outlier_med_dl_11) as cnt_med_dl_11,
sum(cnt_90day - outlier_med_nob_dl_11) as cnt_med_nob_dl_11,
sum(cnt_90day - outlier_med_ibm_11) as cnt_med_ibm_11,
sum(cnt_90day - outlier_med_nob_ibm_11) as cnt_med_nob_ibm_11,
sum(cnt_90day - outlier_med_dl_ibm_11) as cnt_med_dl_ibm_11,
sum(cnt_90day - outlier_med_nob_dl_ibm_11) as cnt_med_nob_dl_ibm_11,
sum(cnt_90day - outlier_ntiles_11) as cnt_ntiles_11,
sum(cnt_90day - outlier_emp_11) as cnt_emp_11,
sum(cnt_90day - outlier_chb_11) as cnt_chb_11,
sum(cnt_90day - outlier_3std_log_11) as cnt_3std_log_11,
sum(cnt_90day - outlier_2std_log_11) as cnt_2std_log_11,

avg(avg_price_med_11) as avg_price_med_11,
avg(avg_price_med_nob_11) as avg_price_med_nob_11,
avg(avg_price_med_dl_11) as avg_price_med_dl_11,
avg(avg_price_med_nob_dl_11) as avg_price_med_nob_dl_11,
avg(avg_price_med_ibm_11) as avg_price_med_ibm_11,
avg(avg_price_med_nob_ibm_11) as avg_price_med_nob_ibm_11,
avg(avg_price_med_dl_ibm_11) as avg_price_med_dl_ibm_11,
avg(avg_price_med_nob_dl_ibm_11) as avg_price_med_nob_dl_ibm_11,
avg(avg_price_ntiles_11) as avg_price_ntiles_11,
avg(avg_price_emp_11) as avg_price_emp_11,
avg(avg_price_chb_11) as avg_price_chb_11,
avg(avg_price_3std_log_11) as avg_price_3std_log_11,
avg(avg_price_2std_log_11) as avg_price_2std_log_11

from public_work_tbls.mp_avgprice_metrics_90_2
group by make
;

select model_year,
sum(case when cnt_90day < 6 then 1 else 0 end) as num_of_styleid_with_lt_6_cars,
sum(case when cnt_90day < 11 then 1 else 0 end) as num_of_styleid_with_lt_11_cars,
sum(case when cnt_90day >=11 then 1 else 0 end) as num_of_styleid_with_ge_11_cars,
count(*) as total_styleid,
sum(cnt_90day) as total_cars,

sum(case when coalesce(outlier_med_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_6,
sum(case when coalesce(outlier_med_nob_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_6,
sum(case when coalesce(outlier_med_dl_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_dl_6,
sum(case when coalesce(outlier_med_nob_dl_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_dl_6,
sum(case when coalesce(outlier_med_ibm_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_ibm_6,
sum(case when coalesce(outlier_med_nob_ibm_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_ibm_6,
sum(case when coalesce(outlier_med_dl_ibm_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_dl_ibm_6,
sum(case when coalesce(outlier_med_nob_dl_ibm_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_dl_ibm_6,
sum(case when coalesce(outlier_ntiles_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_ntiles_6,
sum(case when coalesce(outlier_emp_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_emp_6,
sum(case when coalesce(outlier_chb_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_chb_6,
sum(case when coalesce(outlier_3std_log_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_3std_log_6,
sum(case when coalesce(outlier_2std_log_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_2std_log_6,

sum(outlier_med_6) as outlier_med_6,
sum(outlier_med_nob_6) as outlier_med_nob_6,
sum(outlier_med_dl_6) as outlier_med_dl_6,
sum(outlier_med_nob_dl_6) as outlier_med_nob_dl_6,
sum(outlier_med_ibm_6) as outlier_med_ibm_6,
sum(outlier_med_nob_ibm_6) as outlier_med_nob_ibm_6,
sum(outlier_med_dl_ibm_6) as outlier_med_dl_ibm_6,
sum(outlier_med_nob_dl_ibm_6) as outlier_med_nob_dl_ibm_6,
sum(outlier_ntiles_6) as outlier_ntiles_6,
sum(outlier_emp_6) as outlier_emp_6,
sum(outlier_chb_6) as outlier_chb_6,
sum(outlier_3std_log_6) as outlier_3std_log_6,
sum(outlier_2std_log_6) as outlier_2std_log_6,

sum(cnt_90day - outlier_med_6) as cnt_med_6,
sum(cnt_90day - outlier_med_nob_6) as cnt_med_nob_6,
sum(cnt_90day - outlier_med_dl_6) as cnt_med_dl_6,
sum(cnt_90day - outlier_med_nob_dl_6) as cnt_med_nob_dl_6,
sum(cnt_90day - outlier_med_ibm_6) as cnt_med_ibm_6,
sum(cnt_90day - outlier_med_nob_ibm_6) as cnt_med_nob_ibm_6,
sum(cnt_90day - outlier_med_dl_ibm_6) as cnt_med_dl_ibm_6,
sum(cnt_90day - outlier_med_nob_dl_ibm_6) as cnt_med_nob_dl_ibm_6,
sum(cnt_90day - outlier_ntiles_6) as cnt_ntiles_6,
sum(cnt_90day - outlier_emp_6) as cnt_emp_6,
sum(cnt_90day - outlier_chb_6) as cnt_chb_6,
sum(cnt_90day - outlier_3std_log_6) as cnt_3std_log_6,
sum(cnt_90day - outlier_2std_log_6) as cnt_2std_log_6,

avg(avg_price_med_6) as avg_price_med_6,
avg(avg_price_med_nob_6) as avg_price_med_nob_6,
avg(avg_price_med_dl_6) as avg_price_med_dl_6,
avg(avg_price_med_nob_dl_6) as avg_price_med_nob_dl_6,
avg(avg_price_med_ibm_6) as avg_price_med_ibm_6,
avg(avg_price_med_nob_ibm_6) as avg_price_med_nob_ibm_6,
avg(avg_price_med_dl_ibm_6) as avg_price_med_dl_ibm_6,
avg(avg_price_med_nob_dl_ibm_6) as avg_price_med_nob_dl_ibm_6,
avg(avg_price_ntiles_6) as avg_price_ntiles_6,
avg(avg_price_emp_6) as avg_price_emp_6,
avg(avg_price_chb_6) as avg_price_chb_6,
avg(avg_price_3std_log_6) as avg_price_3std_log_6,
avg(avg_price_2std_log_6) as avg_price_2std_log_6

from public_work_tbls.mp_avgprice_metrics_90_2
group by model_year
;

select make,
sum(case when cnt_90day < 6 then 1 else 0 end) as num_of_styleid_with_lt_6_cars,
sum(case when cnt_90day < 11 then 1 else 0 end) as num_of_styleid_with_lt_11_cars,
sum(case when cnt_90day >=11 then 1 else 0 end) as num_of_styleid_with_ge_11_cars,
count(*) as total_styleid,
sum(cnt_90day) as total_cars,

sum(case when coalesce(outlier_med_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_6,
sum(case when coalesce(outlier_med_nob_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_6,
sum(case when coalesce(outlier_med_dl_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_dl_6,
sum(case when coalesce(outlier_med_nob_dl_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_dl_6,
sum(case when coalesce(outlier_med_ibm_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_ibm_6,
sum(case when coalesce(outlier_med_nob_ibm_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_ibm_6,
sum(case when coalesce(outlier_med_dl_ibm_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_dl_ibm_6,
sum(case when coalesce(outlier_med_nob_dl_ibm_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_med_nob_dl_ibm_6,
sum(case when coalesce(outlier_ntiles_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_ntiles_6,
sum(case when coalesce(outlier_emp_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_emp_6,
sum(case when coalesce(outlier_chb_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_chb_6,
sum(case when coalesce(outlier_3std_log_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_3std_log_6,
sum(case when coalesce(outlier_2std_log_6,0) >0 then 1 else 0 end) as num_of_styleids_with_outlier_2std_log_6,

sum(outlier_med_6) as outlier_med_6,
sum(outlier_med_nob_6) as outlier_med_nob_6,
sum(outlier_med_dl_6) as outlier_med_dl_6,
sum(outlier_med_nob_dl_6) as outlier_med_nob_dl_6,
sum(outlier_med_ibm_6) as outlier_med_ibm_6,
sum(outlier_med_nob_ibm_6) as outlier_med_nob_ibm_6,
sum(outlier_med_dl_ibm_6) as outlier_med_dl_ibm_6,
sum(outlier_med_nob_dl_ibm_6) as outlier_med_nob_dl_ibm_6,
sum(outlier_ntiles_6) as outlier_ntiles_6,
sum(outlier_emp_6) as outlier_emp_6,
sum(outlier_chb_6) as outlier_chb_6,
sum(outlier_3std_log_6) as outlier_3std_log_6,
sum(outlier_2std_log_6) as outlier_2std_log_6,

sum(cnt_90day - outlier_med_6) as cnt_med_6,
sum(cnt_90day - outlier_med_nob_6) as cnt_med_nob_6,
sum(cnt_90day - outlier_med_dl_6) as cnt_med_dl_6,
sum(cnt_90day - outlier_med_nob_dl_6) as cnt_med_nob_dl_6,
sum(cnt_90day - outlier_med_ibm_6) as cnt_med_ibm_6,
sum(cnt_90day - outlier_med_nob_ibm_6) as cnt_med_nob_ibm_6,
sum(cnt_90day - outlier_med_dl_ibm_6) as cnt_med_dl_ibm_6,
sum(cnt_90day - outlier_med_nob_dl_ibm_6) as cnt_med_nob_dl_ibm_6,
sum(cnt_90day - outlier_ntiles_6) as cnt_ntiles_6,
sum(cnt_90day - outlier_emp_6) as cnt_emp_6,
sum(cnt_90day - outlier_chb_6) as cnt_chb_6,
sum(cnt_90day - outlier_3std_log_6) as cnt_3std_log_6,
sum(cnt_90day - outlier_2std_log_6) as cnt_2std_log_6,

avg(avg_price_med_6) as avg_price_med_6,
avg(avg_price_med_nob_6) as avg_price_med_nob_6,
avg(avg_price_med_dl_6) as avg_price_med_dl_6,
avg(avg_price_med_nob_dl_6) as avg_price_med_nob_dl_6,
avg(avg_price_med_ibm_6) as avg_price_med_ibm_6,
avg(avg_price_med_nob_ibm_6) as avg_price_med_nob_ibm_6,
avg(avg_price_med_dl_ibm_6) as avg_price_med_dl_ibm_6,
avg(avg_price_med_nob_dl_ibm_6) as avg_price_med_nob_dl_ibm_6,
avg(avg_price_ntiles_6) as avg_price_ntiles_6,
avg(avg_price_emp_6) as avg_price_emp_6,
avg(avg_price_chb_6) as avg_price_chb_6,
avg(avg_price_3std_log_6) as avg_price_3std_log_6,
avg(avg_price_2std_log_6) as avg_price_2std_log_6

from public_work_tbls.mp_avgprice_metrics_90_2
group by make
;


