


/*** APPROACH 1: Outlier detection based on median and median absolute deviation ***/

/*** 1.1 Calculare median and other descriptive stats ****/

drop table public_work_tbls.mp_base_90;
create table public_work_tbls.mp_base_90 as
select ymm_hash
      ,model_year
      ,dd.division_name as make
      ,md.model_name as model
      ,ct
      ,mean
      ,std
      ,round(avg(case when row_id between ct/2.0 and ct/2.0 + 1 then price end)) as median
      ,min
      ,max
from 
        (
        select
              x.ymm_hash,
              model_year,
              make_id,
              model_id,
              price,
              row_number() over (partition by x.ymm_hash order by price) as row_id,
              y.ct,
              y.mean,
              y.std,
              y.min,
              y.max
        from
                (
                        SELECT a.price,
                        v.model_year,
                        v.make_id,
                        v.model_id,
                        (v.model_year||v.make_id||v.model_id)::bigint AS ymm_hash
                        FROM dw.dealer_vin_price_sale_hist_c a
                        INNER JOIN dw.dealer_c d   
                                ON a.dealer_id = d.dealer_id
                        LEFT OUTER JOIN ods.chrome__vin_c v
                                ON a.vin = v.vin
                        WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL
                        AND vin_last_dt > CURRENT_DATE - 90 
                        AND a.price > 0 
                ) x 
                inner join 
                (
                        select (v.model_year||v.make_id||v.model_id)::bigint AS ymm_hash
                              ,count(*) as ct
                              ,round(avg(price)) as mean
                              ,round(stddev(price)) as std
                              ,round(min(price)) as min
                              ,round(max(price)) as max
                        from dw.dealer_vin_price_sale_hist_c a
                        INNER JOIN dw.dealer_c d   
                                ON a.dealer_id = d.dealer_id
                        LEFT OUTER JOIN ods.chrome__vin_c v
                                ON a.vin = v.vin
                        WHERE --sold_flg AND 
                        latest_record_flg
                        AND v.model_year IS NOT NULL
                        AND vin_last_dt > CURRENT_DATE - 90 
                        AND a.price > 0
                        group by (v.model_year||v.make_id||v.model_id) ::bigint
                ) y 
                on x.ymm_hash=y.ymm_hash) k
                
                inner join ods.chrome_division_definition dd on k.make_id = dd.division_id
                inner join ods.chrome_model_definition md on k.model_id = md.model_id               

group by ymm_hash, ct, mean,min,max, std, model_year, dd.division_name, md.model_name
;


select count(*) from public_work_tbls.mp_base_90;
--3794



/*** 1.2 Calculate Median and Mean Absolute Deviation stats ***/

drop table public_work_tbls.mp_MADs_90;
create table public_work_tbls.mp_MADs_90 as
select  ymm_hash
       ,avg(case when row_id between ct/2.0 and ct/2.0 + 1 then med_dev end ) as MAD
       ,avg(price) as MeanAD

from 
(
select x.ymm_hash
      ,x.price
      ,abs(x.price - median) as med_dev
      ,row_number() over (partition by x.ymm_hash order by abs(x.price - median)) as row_id
      ,y.ct 
from
                (
                        SELECT a.price,
                        (v.model_year||v.make_id||v.model_id)::bigint AS ymm_hash
                        FROM dw.dealer_vin_price_sale_hist_c a
                        INNER JOIN dw.dealer_c d   
                                ON a.dealer_id = d.dealer_id
                        LEFT OUTER JOIN ods.chrome__vin_c v
                                ON a.vin = v.vin
                        WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL
                        AND vin_last_dt > CURRENT_DATE - 90
                        --AND vin_last_dt > CURRENT_DATE - 45 
                        AND a.price > 0 
                ) x 
                inner join public_work_tbls.mp_base_90 y 
                on x.ymm_hash = y.ymm_hash
) z
--where row_id between ct/2.0 and ct/2.0 + 1
group by ymm_hash,ct
;

select count(*) from public_work_tbls.mp_MADs_90;
--3794


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
drop table public_work_tbls.mp_avgprice_med_90;
create table public_work_tbls.mp_avgprice_med_90 as
select  a.ymm_hash
       ,a.model_year
       ,a.make_id
       ,a.model_id
         
      /* conservative (3.5) cut off (both sides) with 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 11 or abs(a.price - b.median)<= 5.189 * MAD then a.price end)) as avg_price_med
      ,sum(case when b.ct < 11 or abs(a.price - b.median)<= 5.189 * MAD then 0 else 1 end) as outlier_med
      ,cast(sum(case when b.ct < 11 or abs(a.price - b.median)<= 5.189 * MAD then 0 else 1 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct

      /* conservative (3.5) cut off (both sides) without adjustment for normality */
      ,round(avg(case when b.ct < 11 or abs(a.price - b.median)<= 3.5 * MAD then a.price end)) as avg_price_med_nob
      ,sum(case when b.ct < 11 or abs(a.price - b.median)<= 3.5 * MAD then 0 else 1 end) as outlier_med_nob
      ,cast(sum(case when b.ct < 11 or abs(a.price - b.median)<= 3.5 * MAD then 0 else 1 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) with 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 11 or (a.price - b.median) between -3.7 * MAD  and 5.189 * MAD then a.price end)) as avg_price_med_dl
      ,sum(case when b.ct < 11 or (a.price - b.median) between -3.7 * MAD  and 5.189 * MAD then 0 else 1 end) as outlier_med_dl
      ,cast(sum(case when b.ct < 11 or (a.price - b.median) between -3.7 * MAD  and 5.189 * MAD then 0 else 1 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_dl

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) without 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 11 or (a.price - b.median) between -2.5 * MAD and 3.5 * MAD then a.price end)) as avg_price_med_nob_dl
      ,sum(case when b.ct < 11 or (a.price - b.median) between -2.5 * MAD and 3.5 * MAD then 0 else 1 end) as outlier_med_nob_dl
      ,cast(sum(case when b.ct < 11 or (a.price - b.median) between -2.5 * MAD and 3.5 * MAD then 0 else 1 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob_dl




      ------------ plus IBM logic for MAD = 0 situations ------------
      /* conservative (3.5) cut off (both sides) with IBM logic for MAD=0 and 1.482 adjustment for normality*/
      ,round(avg(case when b.ct < 11 or abs(a.price - b.median)<= case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then a.price end)) as avg_price_med_ibm
      ,sum(case when b.ct < 11 or abs(a.price - b.median)<= case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then 0 else 1 end) as outlier_med_ibm
      ,cast(sum(case when b.ct < 11 or abs(a.price - b.median)<=case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then 0 else 1 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_ibm

      /* conservative (3.5) cut off (both sides) with IBM logic for MAD=0 and without adjustment for normality */
      ,round(avg(case when b.ct < 11 or abs(a.price - b.median)<=  3.5 * case when MAD=0 then MeanAD else  MAD end then a.price end)) as avg_price_med_nob_ibm
      ,sum(case when b.ct < 11 or abs(a.price - b.median)<=  3.5 * case when MAD=0 then MeanAD else  MAD end then 0 else 1 end) as outlier_med_nob_ibm
      ,cast(sum(case when b.ct < 11 or abs(a.price - b.median)<=  3.5 * case when MAD=0 then MeanAD else  MAD end  then 0 else 1 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob_ibm

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) with IBM logic for MAD=0 and 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 11 or (a.price - b.median) between case when MAD=0 then -3.13 * MeanAD else -3.7 * MAD end  and case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then a.price end)) as avg_price_med_dl_ibm
      ,sum(case when b.ct < 11 or (a.price - b.median) between case when MAD=0 then -3.13 * MeanAD else -3.7 * MAD end  and case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then 0 else 1 end) as outlier_med_dl_ibm
      ,cast(sum(case when b.ct < 11 or (a.price - b.median) between case when MAD=0 then -3.13 * MeanAD else -3.7 * MAD end  and case when MAD=0 then 4.3866 * MeanAD else 5.189 * MAD end then 0 else 1 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_dl_ibm

      /* conservative (3.5) cut off on the right and  less conservative on the left (2.5) with IBM logic for MAD=0 and without 1.482 adjustment for normality */
      ,round(avg(case when b.ct < 11 or (a.price - b.median) between -2.5 * case when MAD=0 then MeanAD else  MAD end and 3.5 * case when MAD=0 then MeanAD else  MAD end then a.price end)) as avg_price_med_nob_dl_ibm
      ,sum(case when b.ct < 11 or (a.price - b.median) between -2.5 * case when MAD=0 then MeanAD else  MAD end and 3.5 * case when MAD=0 then MeanAD else  MAD end then 0 else 1 end) as outlier_med_nob_dl_ibm
      ,cast(sum(case when b.ct < 11 or (a.price - b.median) between -2.5 * case when MAD=0 then MeanAD else  MAD end and 3.5 * case when MAD=0 then MeanAD else  MAD end then 0 else 1 end) as decimal(5,3))/cast(count(*)as decimal(5,3)) as outlier_med_pct_nob_dl_ibm

from                 (
                        SELECT a.price,v.model_year,v.make_id,v.model_id
                        ,(v.model_year||v.make_id||v.model_id)::bigint AS ymm_hash
                        FROM dw.dealer_vin_price_sale_hist_c a
                        INNER JOIN dw.dealer_c d   
                                ON a.dealer_id = d.dealer_id
                        LEFT OUTER JOIN ods.chrome__vin_c v
                                ON a.vin = v.vin
                        WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL
                        AND vin_last_dt > CURRENT_DATE - 90 
                        --AND vin_last_dt > CURRENT_DATE - 45 
                        AND a.price > 0 
                ) a inner join public_work_tbls.mp_base_90 b on a.ymm_hash = b.ymm_hash
                    inner join public_work_tbls.mp_MADs_90 c on a.ymm_hash = c.ymm_hash
group by a.ymm_hash
        ,a.model_year
        ,a.make_id
        ,a.model_id
;

select count(*) from public_work_tbls.mp_avgprice_med_90
;
--3794



/* APPROACH 2: Define outliers as top 1% and bottom 1% centile of the distribution.
Not a great option but distribution agnostic. */

drop table public_work_tbls.mp_avgprice_ntiles_90;
create table public_work_tbls.mp_avgprice_ntiles_90 as
select x.ymm_hash
      ,round(avg(case when b.ct < 11 or centile between 2 and 99 then price end)) as avg_price_ntiles
      ,sum(case when b.ct < 11 or centile between 2 and 99 then 0 else 1 end) as outlier_ntiles
      ,cast(sum(case when b.ct < 11 or centile between 2 and 99 then 0 else 1 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_ntiles_pct

from
        (
        select ymm_hash
              ,price
              ,ntile(100) over (partition by ymm_hash order by price desc) as centile  
        from
                (
                SELECT a.price
                      ,(v.model_year||v.make_id||v.model_id)::bigint AS ymm_hash
                FROM dw.dealer_vin_price_sale_hist_c a
                INNER JOIN dw.dealer_c d   
                        ON a.dealer_id = d.dealer_id
                LEFT OUTER JOIN ods.chrome__vin_c v
                        ON a.vin = v.vin
                WHERE -- sold_flg AND 
                        latest_record_flg
                        AND v.model_year IS NOT NULL
                        AND vin_last_dt > CURRENT_DATE - 90 
                        --AND vin_last_dt > CURRENT_DATE - 45 
                        AND a.price > 0
                ) c       
        ) x inner join public_work_tbls.mp_base_90 b on x.ymm_hash = b.ymm_hash
group by x.ymm_hash

;

select count(*), count(distinct ymm_hash) from public_work_tbls.mp_avgprice_ntiles_90;
--3794 3794




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
drop table public_work_tbls.mp_avgprice_3std_90;
create table public_work_tbls.mp_avgprice_3std_90 as                  
select  x.ymm_hash
    
       ---- The Empirical Rule. Using 3 standard deviations from sample mean ----
       ,sum(case when b.ct < 11 or price between (avg_price - 3*x.std) and (avg_price + 3*x.std) then 0 else 1 end) as outlier_emp
       ,cast (sum(case when b.ct < 11 or price between (avg_price - 3*x.std) and (avg_price + 3*x.std) then 0 else 1 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_emp_pct
       ,round(avg(case when b.ct < 11 or price between (avg_price - 3*x.std) and (avg_price + 3*x.std) then price end)) as avg_price_emp

       ---- Chebyshev's Theorem. Using 4.5 stadard deviations distance from sample mean ----
       ,sum(case when b.ct < 11 or price between (avg_price - 4.5*x.std) and (avg_price + 4.5*x.std) then 0 else 1 end) as outlier_chb
       ,cast (sum(case when b.ct < 11 or price between (avg_price - 4.5*x.std) and (avg_price + 4.5*x.std) then 0 else 1 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_chb_pct
       ,round(avg(case when b.ct < 11 or price between (avg_price - 4.5*x.std) and (avg_price + 4.5*x.std) then price end)) as avg_price_chb
from
        (                  
        select ymm_hash
              ,price 
              ,avg(price) over (partition by ymm_hash order by null) as avg_price    
              ,stddev(price)  over (partition by ymm_hash order by null) as  std
        from 
                (
                 SELECT a.price,
                        (v.model_year||v.make_id||v.model_id)::bigint AS ymm_hash
                FROM dw.dealer_vin_price_sale_hist_c a
                INNER JOIN dw.dealer_c d   
                        ON a.dealer_id = d.dealer_id
                LEFT OUTER JOIN ods.chrome__vin_c v
                        ON a.vin = v.vin
                WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL
                        AND vin_last_dt > CURRENT_DATE - 90 
                        --AND vin_last_dt > CURRENT_DATE - 45 
                        AND a.price > 0
                ) c     
        ) x inner join public_work_tbls.mp_base_90 b on x.ymm_hash = b.ymm_hash
 group by x.ymm_hash
 ;  

select count(*) from public_work_tbls.mp_avgprice_3std_90;
--3794


/* Approach 4: Using Empirical Rule's 2 and 3 standard deviations distance on log-transformed price (for normalization of distribution) */

drop table public_work_tbls.mp_avgprice_3std_log_90;
create table public_work_tbls.mp_avgprice_3std_log_90 as                  
select  x.ymm_hash
       ,sum(case when b.ct < 11 or ln(price) between (avg_logprice - 3*x.std) and (avg_logprice + 3*x.std) then 0 else 1 end) as outlier_3std
       ,cast (sum(case when b.ct < 11 or ln(price) between (avg_logprice - 3*x.std) and (avg_logprice + 3*x.std) then 0 else 1 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_3std_pct
       ,round(avg(case when b.ct < 11 or ln(price) between (avg_logprice - 3*x.std) and (avg_logprice + 3*x.std) then price end)) as avg_price_3std
       
       ,sum(case when b.ct < 11 or ln(price) between (avg_logprice - 2*x.std) and (avg_logprice + 2*x.std) then 0 else 1 end) as outlier_2std
       ,cast (sum(case when b.ct < 11 or ln(price) between (avg_logprice - 2*x.std) and (avg_logprice + 2*x.std) then 0 else 1 end) as decimal(5,3))/cast(count(*) as decimal(5,3)) as outlier_2std_pct
       ,round(avg(case when b.ct < 11 or ln(price) between (avg_logprice - 2*x.std) and (avg_logprice + 2*x.std) then price end)) as avg_price_2std
from
        (                  
        select ymm_hash
              ,price
              ,avg(ln(price)) over (partition by ymm_hash order by null) as avg_logprice    
              ,stddev(ln(price)) over (partition by ymm_hash order by null) as  std
        from 
                (
                 SELECT a.price,
                        (v.model_year||v.make_id||v.model_id)::bigint AS ymm_hash
                FROM dw.dealer_vin_price_sale_hist_c a
                INNER JOIN dw.dealer_c d   
                        ON a.dealer_id = d.dealer_id
                LEFT OUTER JOIN ods.chrome__vin_c v
                        ON a.vin = v.vin
                WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL
                        AND vin_last_dt > CURRENT_DATE - 90 
                        --AND vin_last_dt > CURRENT_DATE - 45 
                        AND a.price > 0
                ) c     
        ) x inner join public_work_tbls.mp_base_90 b on x.ymm_hash = b.ymm_hash
 group by x.ymm_hash
 ;  

select count(*) from public_work_tbls.mp_avgprice_3std_log_90
;
--3789

/*** Put all metrics together ****/

grant select on public_work_tbls.mp_avgprice_metrics_90 to tkozlowski;

drop table public_work_tbls.mp_avgprice_metrics_90;
create table public_work_tbls.mp_avgprice_metrics_90 as
select distinct 
       a.ymm_hash
      ,x.model_year
      ,x.make
      ,x.model

      ,x.ct as cnt_90day
      ,x.mean as avg_price_90day
      ,x.median as median_price_90day
      ,x.min as min_price_90day
      ,x.max as max_price_90day
      ,xx.MAD
      
      ,d.avg_price_med as avg_price_med_90day
      ,d.outlier_med as outlier_med_90day
      ,d.outlier_med_pct as outlier_med_pct_90day
      
      ,d.avg_price_med_nob as avg_price_med_nob_90day
      ,d.outlier_med_nob as outlier_med_nob_90day
      ,d.outlier_med_pct_nob as outlier_med_pct_nob_90day   
 
      ,d.avg_price_med_dl as avg_price_med_dl_90day
      ,d.outlier_med_dl as outlier_med_dl_90day
      ,d.outlier_med_pct_dl as outlier_med_pct_dl_90day  
      
      ,d.avg_price_med_nob_dl as avg_price_med_nob_dl_90day
      ,d.outlier_med_nob_dl as outlier_med_nob_dl_90day
      ,d.outlier_med_pct_nob_dl as outlier_med_pct_nob_dl_90day       
           
           
      ,d.avg_price_med_ibm as avg_price_med_ibm_90day
      ,d.outlier_med_ibm as outlier_med_ibm_90day
      ,d.outlier_med_pct_ibm as outlier_med_pct_ibm_90day
      
      ,d.avg_price_med_nob_ibm as avg_price_med_nob_ibm_90day
      ,d.outlier_med_nob_ibm as outlier_med_nob_ibm_90day
      ,d.outlier_med_pct_nob_ibm as outlier_med_pct_nob_ibm_90day   
 
      ,d.avg_price_med_dl_ibm as avg_price_med_dl_ibm_90day
      ,d.outlier_med_dl_ibm as outlier_med_dl_ibm_90day
      ,d.outlier_med_pct_dl_ibm as outlier_med_pct_dl_ibm_90day  
      
      ,d.avg_price_med_nob_dl_ibm as avg_price_med_nob_dl_ibm_90day
      ,d.outlier_med_nob_dl_ibm as outlier_med_nob_dl_ibm_90day
      ,d.outlier_med_pct_nob_dl_ibm as outlier_med_pct_nob_dl_ibm_90day          
      
     
      
      ,a.avg_price_ntiles as avg_price_ntiles_90day
      ,a.outlier_ntiles as outlier_ntiles_90day
      ,a.outlier_ntiles_pct as outlier_ntiles_pct_90day

      ,b.avg_price_emp as avg_price_emp_90day
      ,b.outlier_emp as outlier_emp_90day
      ,b.outlier_emp_pct as outlier_emp_pct_90day
      
      ,b.avg_price_chb as avg_price_chb_90day
      ,b.outlier_chb as outlier_chb_90day
      ,b.outlier_chb_pct as outlier_chb_pct_90day

      ,c.avg_price_3std as avg_price_3std_log_90day
      ,c.outlier_3std as outlier_3std_log_90day
      ,c.outlier_3std_pct as outlier_3std_pct_log_90day

      ,c.avg_price_2std as avg_price_2std_log_90day
      ,c.outlier_2std as outlier_2std_log_90day
      ,c.outlier_2std_pct as outlier_2std_pct_log_90day

from public_work_tbls.mp_avgprice_ntiles_90 a inner join public_work_tbls.mp_avgprice_3std_90 b on a.ymm_hash=b.ymm_hash
                                              inner join public_work_tbls.mp_avgprice_3std_log_90 c on a.ymm_hash=c.ymm_hash
                                              inner join public_work_tbls.mp_avgprice_med_90 d on a.ymm_hash=d.ymm_hash
                                              inner join public_work_tbls.mp_base_90 x on a.ymm_hash=x.ymm_hash
                                              inner join public_work_tbls.mp_MADs_90 xx on a.ymm_hash = xx.ymm_hash

                              
;

select count(*) from public_work_tbls.mp_avgprice_metrics_90;
;
--3789



/**** Calculate Mean Absolute Error for each of 14 methods for comparison 
       MAE = sum (abs(price - price_hat))/ n
       
Also compare different windows for analysis (45 days, 90 days, 120 days, "current date", and all)       
***/
/*
drop table public_work_tbls.mp_choose_one_90;
drop table public_work_tbls.mp_choose_one_120;
drop table public_work_tbls.mp_choose_one_45;
drop table public_work_tbls.mp_choose_one_all;
drop table public_work_tbls.mp_choose_one_curr;
*/
drop table public_work_tbls.mp_choose_one;
create table public_work_tbls.mp_choose_one as

select avg(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end) as price,
avg(a.price) as price_rough,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_1,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_med_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_2,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_med_nob_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_3,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_med_dl_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_4,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_med_nob_dl_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_5,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_med_ibm_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_6,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_med_nob_ibm_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_7,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_med_dl_ibm_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_8,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_med_nob_dl_ibm_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_9,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_ntiles_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_10,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_emp_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_11,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_chb_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_12,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_3std_log_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_13,
cast(sum(abs(case when cnt_90day < 11 or abs(a.price - median_price_90day)<= 3.5 * MAD then a.price end- avg_price_2std_log_90day)) as decimal(6,4))/cast(count(*) as decimal(6,4)) as MAE_14
from (
                        SELECT a.price,
                        v.model_year,
                        v.make_id,
                        v.model_id,
                        (v.model_year||v.make_id||v.model_id)::bigint AS ymm_hash
                        FROM dw.dealer_vin_price_sale_hist_c a
                        INNER JOIN dw.dealer_c d   
                                ON a.dealer_id = d.dealer_id
                        LEFT OUTER JOIN ods.chrome__vin_c v
                                ON a.vin = v.vin
                        WHERE --sold_flg AND
                        latest_record_flg
                        AND v.model_year IS NOT NULL
                        AND vin_last_dt > CURRENT_DATE -90
                        --AND vin_last_dt = CURRENT_DATE 
                        --AND vin_last_dt > CURRENT_DATE  - 45
                        --AND vin_last_dt > CURRENT_DATE  - 90
                        --AND vin_last_dt > CURRENT_DATE  - 120
                        AND a.price > 0 
                ) a left outer join public_work_tbls.mp_avgprice_metrics_90 z on a.ymm_hash = z.ymm_hash

;

select * from public_work_tbls.mp_choose_one
;

/*
select '90' as dim, * from public_work_tbls.mp_choose_one_90
union all
select '120' as dim, * from public_work_tbls.mp_choose_one_120
union all
select '45' as dim, * from public_work_tbls.mp_choose_one_45
union all
select 'all' as dim, * from public_work_tbls.mp_choose_one_all
union all
select 'curr' as dim, * from public_work_tbls.mp_choose_one_curr
;
*/
    
select 
model_year, make,
avg(outlier_med_90day) as outlier_med_90day,
avg(outlier_med_pct_90day) as outlier_med_pct_90day,
avg(outlier_med_nob_90day) as outlier_med_nob_90day,
avg(outlier_med_pct_nob_90day) as outlier_med_pct_nob_90day,
avg(outlier_med_dl_90day) as outlier_med_dl_90day,
avg(outlier_med_pct_dl_90day) as outlier_med_pct_dl_90day,
avg(outlier_med_nob_dl_90day) as outlier_med_nob_dl_90day,
avg(outlier_med_pct_nob_dl_90day) as outlier_med_pct_nob_dl_90day,
avg(outlier_med_ibm_90day) as outlier_med_ibm_90day,
avg(outlier_med_pct_ibm_90day) as outlier_med_pct_ibm_90day,
avg(outlier_med_nob_ibm_90day) as outlier_med_nob_ibm_90day,
avg(outlier_med_pct_nob_ibm_90day) as outlier_med_pct_nob_ibm_90day,
avg(outlier_med_dl_ibm_90day) as outlier_med_dl_ibm_90day,
avg(outlier_med_pct_dl_ibm_90day) as outlier_med_pct_dl_ibm_90day,
avg(outlier_med_nob_dl_ibm_90day) as outlier_med_nob_dl_ibm_90day,
avg(outlier_med_pct_nob_dl_ibm_90day) as outlier_med_pct_nob_dl_ibm_90day,
avg(outlier_ntiles_90day) as outlier_ntiles_90day,
avg(outlier_ntiles_pct_90day) as outlier_ntiles_pct_90day,
avg(outlier_emp_90day) as outlier_emp_90day,
avg(outlier_emp_pct_90day) as outlier_emp_pct_90day,
avg(outlier_chb_90day) as outlier_chb_90day,
avg(outlier_chb_pct_90day) as outlier_chb_pct_90day,
avg(outlier_3std_log_90day) as outlier_3std_log_90day,
avg(outlier_3std_pct_log_90day) as outlier_3std_pct_log_90day,
avg(outlier_2std_log_90day) as outlier_2std_log_90day,
avg(outlier_2std_pct_log_90day) as outlier_2std_pct_log_90day
from public_work_tbls.mp_avgprice_metrics_90
group by model_year, make
;
    
    


                  