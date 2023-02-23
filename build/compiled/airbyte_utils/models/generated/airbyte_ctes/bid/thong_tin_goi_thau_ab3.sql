
with __dbt__cte__thong_tin_goi_thau_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: bid._airbyte_raw_thong_tin_goi_thau
select
    json_value(_airbyte_data, 
    '$."id"' RETURNING CHAR) as id,
    json_value(_airbyte_data, 
    '$."link"' RETURNING CHAR) as link,
    json_value(_airbyte_data, 
    '$."deleted"' RETURNING CHAR) as deleted,
    json_value(_airbyte_data, 
    '$."loai_tb"' RETURNING CHAR) as loai_tb,
    json_value(_airbyte_data, 
    '$."so_tbmt"' RETURNING CHAR) as so_tbmt,
    json_value(_airbyte_data, 
    '$."linh_vuc"' RETURNING CHAR) as linh_vuc,
    json_value(_airbyte_data, 
    '$."services"' RETURNING CHAR) as services,
    json_value(_airbyte_data, 
    '$."nguon_von"' RETURNING CHAR) as nguon_von,
    json_value(_airbyte_data, 
    '$."phan_loai"' RETURNING CHAR) as phan_loai,
    json_value(_airbyte_data, 
    '$."tien_dbdt"' RETURNING CHAR) as tien_dbdt,
    json_value(_airbyte_data, 
    '$."timestamp"' RETURNING CHAR) as `timestamp`,
    json_value(_airbyte_data, 
    '$."company_id"' RETURNING CHAR) as company_id,
    json_value(_airbyte_data, 
    '$."ten_khlcnt"' RETURNING CHAR) as ten_khlcnt,
    json_value(_airbyte_data, 
    '$."trang_thai"' RETURNING CHAR) as trang_thai,
    json_value(_airbyte_data, 
    '$."gia_du_toan"' RETURNING CHAR) as gia_du_toan,
    json_value(_airbyte_data, 
    '$."ten_du_toan"' RETURNING CHAR) as ten_du_toan,
    json_value(_airbyte_data, 
    '$."ben_moi_thau"' RETURNING CHAR) as ben_moi_thau,
    json_value(_airbyte_data, 
    '$."gia_goi_thau"' RETURNING CHAR) as gia_goi_thau,
    json_value(_airbyte_data, 
    '$."hinh_thuc_tb"' RETURNING CHAR) as hinh_thuc_tb,
    json_value(_airbyte_data, 
    '$."send_mail_id"' RETURNING CHAR) as send_mail_id,
    json_value(_airbyte_data, 
    '$."ten_goi_thau"' RETURNING CHAR) as ten_goi_thau,
    json_value(_airbyte_data, 
    '$."competitor_id"' RETURNING CHAR) as competitor_id,
    json_value(_airbyte_data, 
    '$."loai_hop_dong"' RETURNING CHAR) as loai_hop_dong,
    json_value(_airbyte_data, 
    '$."score_by_name"' RETURNING CHAR) as score_by_name,
    json_value(_airbyte_data, 
    '$."score_service"' RETURNING CHAR) as score_service,
    json_value(_airbyte_data, 
    '$."gia_trung_thau"' RETURNING CHAR) as gia_trung_thau,
    json_value(_airbyte_data, 
    '$."hinh_thuc_dbdt"' RETURNING CHAR) as hinh_thuc_dbdt,
    json_value(_airbyte_data, 
    '$."hinh_thuc_lcnt"' RETURNING CHAR) as hinh_thuc_lcnt,
    json_value(_airbyte_data, 
    '$."ngay_phe_duyet"' RETURNING CHAR) as ngay_phe_duyet,
    json_value(_airbyte_data, 
    '$."score_by_scope"' RETURNING CHAR) as score_by_scope,
    json_value(_airbyte_data, 
    '$."so_hieu_khlcnt"' RETURNING CHAR) as so_hieu_khlcnt,
    json_value(_airbyte_data, 
    '$."hieu_luc_e_hsdt"' RETURNING CHAR) as hieu_luc_e_hsdt,
    json_value(_airbyte_data, 
    '$."so_tbmt_version"' RETURNING CHAR) as so_tbmt_version,
    json_value(_airbyte_data, 
    '$."dia_diem_mo_thau"' RETURNING CHAR) as dia_diem_mo_thau,
    json_value(_airbyte_data, 
    '$."phuong_thuc_lcnt"' RETURNING CHAR) as phuong_thuc_lcnt,
    json_value(_airbyte_data, 
    '$."services_by_name"' RETURNING CHAR) as services_by_name,
    json_value(_airbyte_data, 
    '$."don_vi_trung_thau"' RETURNING CHAR) as don_vi_trung_thau,
    json_value(_airbyte_data, 
    '$."hinh_thuc_du_thau"' RETURNING CHAR) as hinh_thuc_du_thau,
    json_value(_airbyte_data, 
    '$."services_by_scope"' RETURNING CHAR) as services_by_scope,
    json_value(_airbyte_data, 
    '$."dia_diem_nhan_hsdt"' RETURNING CHAR) as dia_diem_nhan_hsdt,
    json_value(_airbyte_data, 
    '$."dia_diem_thuc_hien"' RETURNING CHAR) as dia_diem_thuc_hien,
    json_value(_airbyte_data, 
    '$."nhan_e_hsdt_tu_ngay"' RETURNING CHAR) as nhan_e_hsdt_tu_ngay,
    json_value(_airbyte_data, 
    '$."thoi_gian_thuc_hien"' RETURNING CHAR) as thoi_gian_thuc_hien,
    json_value(_airbyte_data, 
    '$."nhan_e_hsdt_den_ngay"' RETURNING CHAR) as nhan_e_hsdt_den_ngay,
    json_value(_airbyte_data, 
    '$."thoi_diem_dong_mo_thau"' RETURNING CHAR) as thoi_diem_dong_mo_thau,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    
    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from bid._airbyte_raw_thong_tin_goi_thau as table_alias
-- thong_tin_goi_thau
where 1 = 1
),  __dbt__cte__thong_tin_goi_thau_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__thong_tin_goi_thau_ab1
select
    cast(id as 
    signed
) as id,
    cast(link as char(1024)) as link,
    cast(deleted as 
    signed
) as deleted,
    cast(loai_tb as char(1024)) as loai_tb,
    cast(so_tbmt as char(1024)) as so_tbmt,
    cast(linh_vuc as char(1024)) as linh_vuc,
    cast(services as char(6144)) as services,
    cast(nguon_von as char(1024)) as nguon_von,
    cast(phan_loai as char(1024)) as phan_loai,
    cast(tien_dbdt as char(1024)) as tien_dbdt,
    cast(`timestamp` as char(1024)) as `timestamp`,
    cast(company_id as char(1024)) as company_id,
    cast(ten_khlcnt as char(1024)) as ten_khlcnt,
    cast(trang_thai as 
    signed
) as trang_thai,
    cast(gia_du_toan as char(1024)) as gia_du_toan,
    cast(ten_du_toan as char(1024)) as ten_du_toan,
    cast(ben_moi_thau as char(1024)) as ben_moi_thau,
    cast(gia_goi_thau as char(1024)) as gia_goi_thau,
    cast(hinh_thuc_tb as char(1024)) as hinh_thuc_tb,
    cast(send_mail_id as char(1024)) as send_mail_id,
    cast(ten_goi_thau as char(1024)) as ten_goi_thau,
    cast(competitor_id as char(1024)) as competitor_id,
    cast(loai_hop_dong as char(1024)) as loai_hop_dong,
    cast(score_by_name as 
    float
) as score_by_name,
    cast(score_service as 
    float
) as score_service,
    cast(gia_trung_thau as char(1024)) as gia_trung_thau,
    cast(hinh_thuc_dbdt as char(1024)) as hinh_thuc_dbdt,
    cast(hinh_thuc_lcnt as char(1024)) as hinh_thuc_lcnt,
    cast(ngay_phe_duyet as char(1024)) as ngay_phe_duyet,
    cast(score_by_scope as 
    float
) as score_by_scope,
    cast(so_hieu_khlcnt as char(1024)) as so_hieu_khlcnt,
    cast(hieu_luc_e_hsdt as char(1024)) as hieu_luc_e_hsdt,
    cast(so_tbmt_version as char(1024)) as so_tbmt_version,
    cast(dia_diem_mo_thau as char(1024)) as dia_diem_mo_thau,
    cast(phuong_thuc_lcnt as char(1024)) as phuong_thuc_lcnt,
    cast(services_by_name as char(6144)) as services_by_name,
    cast(don_vi_trung_thau as char(1024)) as don_vi_trung_thau,
    cast(hinh_thuc_du_thau as char(1024)) as hinh_thuc_du_thau,
    cast(services_by_scope as char(6144)) as services_by_scope,
    cast(dia_diem_nhan_hsdt as char(1024)) as dia_diem_nhan_hsdt,
    cast(dia_diem_thuc_hien as char(1024)) as dia_diem_thuc_hien,
    cast(nhan_e_hsdt_tu_ngay as char(1024)) as nhan_e_hsdt_tu_ngay,
    cast(thoi_gian_thuc_hien as char(1024)) as thoi_gian_thuc_hien,
    cast(nhan_e_hsdt_den_ngay as char(1024)) as nhan_e_hsdt_den_ngay,
    cast(thoi_diem_dong_mo_thau as char(1024)) as thoi_diem_dong_mo_thau,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    
    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from __dbt__cte__thong_tin_goi_thau_ab1
-- thong_tin_goi_thau
where 1 = 1
)-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__thong_tin_goi_thau_ab2
select
    md5(cast(concat(coalesce(cast(id as char), ''), '-', coalesce(cast(link as char), ''), '-', coalesce(cast(deleted as char), ''), '-', coalesce(cast(loai_tb as char), ''), '-', coalesce(cast(so_tbmt as char), ''), '-', coalesce(cast(linh_vuc as char), ''), '-', coalesce(cast(services as char), ''), '-', coalesce(cast(nguon_von as char), ''), '-', coalesce(cast(phan_loai as char), ''), '-', coalesce(cast(tien_dbdt as char), ''), '-', coalesce(cast(`timestamp` as char), ''), '-', coalesce(cast(company_id as char), ''), '-', coalesce(cast(ten_khlcnt as char), ''), '-', coalesce(cast(trang_thai as char), ''), '-', coalesce(cast(gia_du_toan as char), ''), '-', coalesce(cast(ten_du_toan as char), ''), '-', coalesce(cast(ben_moi_thau as char), ''), '-', coalesce(cast(gia_goi_thau as char), ''), '-', coalesce(cast(hinh_thuc_tb as char), ''), '-', coalesce(cast(send_mail_id as char), ''), '-', coalesce(cast(ten_goi_thau as char), ''), '-', coalesce(cast(competitor_id as char), ''), '-', coalesce(cast(loai_hop_dong as char), ''), '-', coalesce(cast(score_by_name as char), ''), '-', coalesce(cast(score_service as char), ''), '-', coalesce(cast(gia_trung_thau as char), ''), '-', coalesce(cast(hinh_thuc_dbdt as char), ''), '-', coalesce(cast(hinh_thuc_lcnt as char), ''), '-', coalesce(cast(ngay_phe_duyet as char), ''), '-', coalesce(cast(score_by_scope as char), ''), '-', coalesce(cast(so_hieu_khlcnt as char), ''), '-', coalesce(cast(hieu_luc_e_hsdt as char), ''), '-', coalesce(cast(so_tbmt_version as char), ''), '-', coalesce(cast(dia_diem_mo_thau as char), ''), '-', coalesce(cast(phuong_thuc_lcnt as char), ''), '-', coalesce(cast(services_by_name as char), ''), '-', coalesce(cast(don_vi_trung_thau as char), ''), '-', coalesce(cast(hinh_thuc_du_thau as char), ''), '-', coalesce(cast(services_by_scope as char), ''), '-', coalesce(cast(dia_diem_nhan_hsdt as char), ''), '-', coalesce(cast(dia_diem_thuc_hien as char), ''), '-', coalesce(cast(nhan_e_hsdt_tu_ngay as char), ''), '-', coalesce(cast(thoi_gian_thuc_hien as char), ''), '-', coalesce(cast(nhan_e_hsdt_den_ngay as char), ''), '-', coalesce(cast(thoi_diem_dong_mo_thau as char), '')) as char)) as _airbyte_thong_tin_goi_thau_hashid,
    tmp.*
from __dbt__cte__thong_tin_goi_thau_ab2 tmp
-- thong_tin_goi_thau
where 1 = 1