SELECT continent_code, country_code, ISO3166_2region1, EXTRACT(YEAR from date) as year, ARRAY_AGG(
    struct(
        date,
        bucket_min,
        bucket_max,
        dl_frac,
        dl_samples,
        ul_frac,
        ul_samples,
        download_MIN,
        download_Q25,
        download_MED,
        download_AVG,
        download_Q75,
        download_MAX,
        dl_total_samples,
        upload_MIN,
        upload_Q25,
        upload_MED,
        upload_AVG,
        upload_Q75,
        upload_MAX,
        ul_total_samples
    ) order by date) as histograms
FROM {{ .sourceTable }}
{{ .whereClause }}
GROUP BY continent_code, country_code, ISO3166_2region1, year