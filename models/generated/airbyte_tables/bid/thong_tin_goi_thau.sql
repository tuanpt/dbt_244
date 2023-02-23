{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "bid",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='thong_tin_goi_thau_scd'
                        )
                    %}
                    {%
                        if scd_table_relation is not none
                    %}
                    {%
                            do adapter.drop_relation(scd_table_relation)
                    %}
                    {% endif %}
                        "],
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('thong_tin_goi_thau_ab3') }}
select
    id,
    link,
    deleted,
    loai_tb,
    so_tbmt,
    linh_vuc,
    services,
    nguon_von,
    phan_loai,
    tien_dbdt,
    {{ adapter.quote('timestamp') }},
    company_id,
    ten_khlcnt,
    trang_thai,
    gia_du_toan,
    ten_du_toan,
    ben_moi_thau,
    gia_goi_thau,
    hinh_thuc_tb,
    send_mail_id,
    ten_goi_thau,
    competitor_id,
    loai_hop_dong,
    score_by_name,
    score_service,
    gia_trung_thau,
    hinh_thuc_dbdt,
    hinh_thuc_lcnt,
    ngay_phe_duyet,
    score_by_scope,
    so_hieu_khlcnt,
    hieu_luc_e_hsdt,
    so_tbmt_version,
    dia_diem_mo_thau,
    phuong_thuc_lcnt,
    services_by_name,
    don_vi_trung_thau,
    hinh_thuc_du_thau,
    services_by_scope,
    dia_diem_nhan_hsdt,
    dia_diem_thuc_hien,
    nhan_e_hsdt_tu_ngay,
    thoi_gian_thuc_hien,
    nhan_e_hsdt_den_ngay,
    thoi_diem_dong_mo_thau,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_thong_tin_goi_thau_hashid
from {{ ref('thong_tin_goi_thau_ab3') }}
-- thong_tin_goi_thau from {{ source('bid', '_airbyte_raw_thong_tin_goi_thau') }}
where 1 = 1

