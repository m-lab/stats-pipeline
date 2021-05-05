SELECT * FROM {{ .sourceTable }}
WHERE {{ .whereClause }}