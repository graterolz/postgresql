DROP FUNCTION sbspla.up_retencionesjudiciales_traer_data (
	p_query text,
	p_id bigint,
	p_pageNumber smallint,
	p_pageSize smallint,
	p_sortField varchar,
	p_sortOrder varchar
);
--
CREATE OR REPLACE FUNCTION sbspla.up_retencionesjudiciales_traer_data (
	IN p_query text,
	IN p_id bigint,
	IN p_pageNumber smallint,
	IN p_pageSize smallint,
	IN p_sortField varchar = ''::character varying,
	IN p_sortOrder varchar = ''::character varying
)
RETURNS TABLE 
(
	id bigint,
	numero_resolucion varchar,
	persona_id bigint,
	fecha_inicio timestamp WITH TIME ZONE,
	fecha_fin timestamp WITH TIME ZONE,
	importe numeric,
	porcentaje numeric,
	comentario text,
	razon_social varchar
)
AS
$$
DECLARE
	strsql text;
	limitValue integer = 0;
	offsetValue integer = 0;
BEGIN
	IF (p_pageNumber IS NULL OR LENGTH(TRIM(p_pageNumber::varchar)) = 0) THEN
		p_pageNumber := 0;
	END IF;
	IF (p_pageSize IS NULL OR LENGTH(TRIM(p_pageSize::varchar)) = 0) THEN
		p_pageSize := 0;
	END IF;
	IF (p_pageNumber < 1) THEN
		p_pageNumber := 1;
	END IF;
	IF (p_sortField IS NULL OR LENGTH(TRIM(p_sortField)) = 0) THEN
		p_sortField := '';
	END IF;
	IF (p_sortOrder IS NULL OR LENGTH(TRIM(p_sortOrder)) = 0) THEN
		p_sortOrder := '';
	END IF;
	--
	limitValue := p_pageSize;
	offsetValue := (p_pageNumber - 1) * p_pageSize;
	--
	strsql :=
	'SELECT
		rd.id,
		rd.numero_resolucion,
		rd.persona_id,
		rd.fecha_inicio,
		rd.fecha_fin,
		rd.importe,
		rd.porcentaje,
		rd.comentario,
		per.razon_social_nombres AS razon_social 
	FROM sbspla.retencionjudicial rd
	INNER JOIN sbsep.view_persona per ON rd.persona_id = per.id
	WHERE rd.estado <> 0
	AND per.estado <> 0';
	--
	IF (p_id IS NOT NULL OR LENGTH(TRIM(p_id::varchar)) > 0) THEN
		strsql:= strsql || ' AND rd.id = $2 ';
	END IF;
	--
	strsql := strsql || '
		AND ($1 = '''' OR $1 IS NULL
		OR rd.numero_resolucion ILIKE ''%'' || trim($1) || ''%''
		OR per.razon_social_nombres ILIKE ''%'' || trim($1) || ''%'')';
	--
	IF LENGTH(TRIM(p_sortField)) > 0 AND LENGTH(TRIM(p_sortOrder)) > 0 THEN
		strsql := strsql || ' ORDER BY ' || p_sortField || ' ' || p_sortOrder || ' ';
	END IF;
	--
	IF (limitValue > 0 AND offsetValue >= 0) THEN
		strsql := strsql || ' LIMIT ' || limitValue || ' OFFSET ' || offsetValue || ' ';
	END IF;
	--
	RAISE NOTICE 'SQL( % ) FIN.',strsql;
	RETURN QUERY EXECUTE strsql USING p_query, p_id;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;