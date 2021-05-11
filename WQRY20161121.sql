DROP FUNCTION sbspla.up_retencionesjudiciales_traer_data_x_persona(
	p_persona_id bigint,p_page_number smallint,p_page_size smallint
);
--
CREATE OR REPLACE FUNCTION sbspla.up_retencionesjudiciales_traer_data_x_persona (
	IN p_persona_id bigint,
	IN p_page_number smallint,
	IN p_page_size smallint
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
	comentario text
)
AS
$$
BEGIN
	IF (p_page_number IS NULL OR LENGTH(TRIM(p_page_number::VARCHAR)) = 0) THEN
		p_page_number := 0;
	END IF;

	IF (p_page_size IS NULL OR LENGTH(TRIM(p_page_size::VARCHAR)) = 0) THEN
		p_page_size := 0;
	END IF;

	IF (p_page_number < 1) THEN
		p_page_number := 1;
	END IF;

	IF (p_page_size = 0 OR p_page_number = 0) THEN
		RETURN QUERY
		SELECT
			rd.id,rd.numero_resolucion,rd.persona_id,rd.fecha_inicio,
			rd.fecha_fin,rd.importe,rd.porcentaje,rd.comentario
		FROM sbspla.retencionjudicial rd
		WHERE rd.estado = 1
		AND rd.persona_id = p_persona_id
		ORDER BY rd.fecha_inicio;
	ELSE
		RETURN QUERY
		SELECT
			rd.id,rd.numero_resolucion,rd.persona_id,rd.fecha_inicio,
			rd.fecha_fin,rd.importe,rd.porcentaje,rd.comentario
		FROM sbspla.retencionjudicial rd
		WHERE rd.estado = 1
		AND rd.persona_id = p_persona_id
		ORDER BY rd.fecha_inicio
		LIMIT p_page_size OFFSET ((p_page_number - 1) * p_page_size);
	END IF;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;
--
DROP FUNCTION sbsep.up_personadireccion_search_by (
	p_personaid bigint,
	p_culture text,
	p_direccionid bigint,
	p_pageNumber smallint,
	p_pageSize smallint,
	p_sortField varchar,
	p_sortOrder varchar
);
--
CREATE OR REPLACE FUNCTION sbsep.up_personadireccion_search_by (	
	IN p_personaid bigint,
	IN p_culture text,
	IN p_direccionid bigint,
	IN p_pageNumber smallint,
	IN p_pageSize smallint,
	IN p_sortField varchar = ''::character varying,
	IN p_sortOrder varchar = ''::character varying
)
RETURNS TABLE 
(
	id bigint,
	persona_id bigint,
	ubigeo_id bigint,
	referencia varchar,
	estado smallint,
	tipozona_id bigint,
	nombre_zona text,
	tipovia_id bigint,
	nombre_via text,
	numero_via text,
	interior_domicilio text,
	tipodireccion_id bigint,
	alias text,
	nombre_tipodire varchar,
	abreviatura_tipozona varchar,
	abreviatura_tipovia varchar,
	nombre_tipovia varchar,
	zona_grid varchar,
	direccion_grid varchar,
	ubigeo_nombre varchar	
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

	limitValue := p_pageSize;
	offsetValue := (p_pageNumber - 1) * p_pageSize;
	
	strsql := 
	'SELECT
		dire.id,
		dire.persona_id,
		dire.ubigeo_id,
		dire.referencia,
		dire.estado,
		dire.tipozona_id,
		dire.nombre_zona,
		dire.tipovia_id,
		dire.nombre_via,
		dire.numero_via,
		dire.interior_domicilio,
		dire.tipodireccion_id,
		dire.alias,			
		(CASE WHEN (nombre_tipodire.valor)  = ''Otros'' THEN alias ELSE (nombre_tipodire.valor) END) AS nombre_tipodire,			
		abreviatura_tipozona.valor::VARCHAR AS abreviatura_tipozona,			
		abreviatura_tipovia.valor::VARCHAR AS abreviatura_tipovia,
		nombre_tipovia.valor::VARCHAR AS nombre_tipovia,
		(abreviatura_tipozona.valor||'' ''||dire.nombre_zona)::VARCHAR AS zona_grid,
		TRIM(COALESCE(abreviatura_tipovia.valor,nombre_tipovia.valor)||
		'' ''||
		dire.nombre_via
		||'' ''||
		dire.numero_via||
		COALESCE(			
		(CASE COALESCE(dire.interior_domicilio,'''') 
			WHEN '''' THEN NULL 
			ELSE '' INT ''||dire.interior_domicilio 
		END),
		'''')
		)::VARCHAR AS direccion_grid,				
		(ubigeo.nombre_dis || '' , '' || ubigeo.nombre_pro|| '' , ''|| ubigeo.nombre_dep)::VARCHAR AS ubigeo_nombre
	FROM sbsep.personadireccion AS dire
	INNER JOIN sbsep.ubigeo as ubigeo ON  ubigeo.id = dire.ubigeo_id
	INNER JOIN sbssys.enumeracion as enu ON enu.id = dire.tipodireccion_id
	INNER JOIN sbssys.up_idiomacontenido_traer_in_culture_x_id($2,enu.nombre_id) nombre_tipodire ON TRUE
	LEFT JOIN sbssys.enumeracion entipozona ON entipozona.id = dire.tipozona_id
	LEFT JOIN sbssys.up_idiomacontenido_traer_in_culture_x_id($2,entipozona.abreviatura_id) abreviatura_tipozona ON TRUE
	INNER JOIN sbssys.enumeracion entipovia ON entipovia.id = dire.tipovia_id
	LEFT JOIN sbssys.up_idiomacontenido_traer_in_culture_x_id($2,entipovia.abreviatura_id) abreviatura_tipovia ON TRUE
	INNER JOIN sbssys.up_idiomacontenido_traer_in_culture_x_id($2,entipovia.nombre_id) nombre_tipovia ON TRUE
	WHERE dire.estado = 1
	AND dire.persona_id = $1';
		
	IF (p_direccionid IS NOT NULL OR LENGTH(TRIM(p_direccionid::varchar)) > 0) THEN
		strsql:= strsql || ' AND dire.id = $3';
	END IF;
	
	IF LENGTH(TRIM(p_sortField)) > 0 AND LENGTH(TRIM(p_sortOrder)) > 0 THEN
		strsql := strsql ||
			' ORDER BY ' || p_sortField || ' ' || p_sortOrder || ' ';
	END IF;

	IF (limitValue > 0 AND offsetValue >= 0) THEN
		strsql := strsql ||
			' LIMIT ' || limitValue || ' OFFSET ' || offsetValue || ' ';
	END IF;	
	
	RAISE NOTICE 'SQL( % ) FIN.', strsql;	
	RETURN QUERY EXECUTE strsql USING p_personaid, p_culture, p_direccionid;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;