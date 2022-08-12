BEGIN
CREATE OR REPLACE TABLE `i-dss-cdm-data-dev.bq_temp.{{table_name}}` as

SELECT
  dw.v69_registration_id_nbr,
  v126_profile_id,
  master_profile_ind,
  profile_type_cd,
  deleted_ind,
  geo_zip_cd,
  case when mc_visitor_id_nbr='00000000000000000000000000000000000000' then post_visitor_id
       else mc_visitor_id_nbr
       end as mc_visitor_id_nbr,
  dw.mobile_id,
  post_visitor_id,
  dw.device_type_nm,
  subscription_package_desc,
  dw.post_ip_address_desc,
  g.internal_point_lon int_point_lon,
  g.internal_point_lat int_point_lat,
  geo_city_nm,
  g.state_code,
  mp.video_show_nm,
  mp.video_genre_nm,
  day_dt,
  mp.video_title,
  mp.video_episode_nbr,
  dw.v31_mpx_reference_guid,
  dw.strm_start_event_dt_ht,
  dw.strm_end_event_dt_ht,
  dw.livetv_affiliate_nm,
  ddm.device_full_nm,
  ddm.device_os_nm
FROM
  `i-dss-cdm-data.cdm_vw.aa_video_detail_reporting_day`  AS dw
JOIN
  `i-dss-cdm-data.dw_vw.mpx_video_content` AS mp
ON
  dw.v31_mpx_reference_guid =mp.src_video_guid
JOIN
  `bigquery-public-data.geo_us_boundaries.zip_codes` AS g
ON
  dw.geo_zip_cd= g.zip_code
JOIN
  `i-dss-cdm-data.dw_vw.da_device_dim` AS ddm
ON
  dw.mobile_id= ddm.da_device_id
LEFT JOIN 
`i-dss-cdm-data.cdm_vw.cbs_aa_user_profile_map` p_map
on (dw.v69_registration_id_nbr=p_map.cbs_reg_user_id_cd and dw.v126_profile_id=CAST(p_map.src_id as string) and p_map.src_system_id=115)
WHERE
  (dw.v69_registration_id_nbr IN (
SELECT distinct v69_registration_id_nbr 
FROM `i-dss-cdm-data-dev.jay_sandbox.sub_abuse_big_data` limit {{sample_size}})
    AND dw.day_dt>='{{start_dt}}'
    AND dw.day_dt<='{{end_dt}}'
   AND report_suite_id_nm='cnetcbscomsite'
    AND ((dw.video_start_cnt>0
    AND dw.video_content_duration_sec_qty>0) or (dw.video_content_duration_sec_qty>0) ) )
GROUP BY
  dw.v69_registration_id_nbr,
  v126_profile_id,
  master_profile_ind,
  profile_type_cd,
  deleted_ind,
  geo_zip_cd,
  mc_visitor_id_nbr,
  dw.mobile_id,
  post_visitor_id,
  dw.device_type_nm,
  subscription_package_desc,
  dw.post_ip_address_desc,
  g.internal_point_lon ,
  g.internal_point_lat ,
  geo_city_nm,
  g.state_code, 
  mp.video_show_nm,
  mp.video_genre_nm,
  day_dt,
  mp.video_title,
  mp.video_episode_nbr,
  dw.v31_mpx_reference_guid, 
  dw.strm_start_event_dt_ht,
  dw.strm_end_event_dt_ht,
  dw.livetv_affiliate_nm,
  ddm.device_full_nm,
  ddm.device_os_nm
ORDER BY
  dw.v69_registration_id_nbr,
  dw.strm_start_event_dt_ht;


EXPORT DATA OPTIONS(
  uri='gs://{{bucket_name}}/sub_abuse/{{table_name}}_*.csv.gz',
  format='CSV',
  overwrite=true,
  header=true,
  compression='GZIP',
  field_delimiter=',') AS
  SELECT * FROM `i-dss-cdm-data-dev.bq_temp.{{table_name}}`;


END;