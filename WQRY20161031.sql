DROP FUNCTION public.up_solicitudsunat_search_by(
	culture varchar,
	empresaid bigint,
	estadobusquedaid smallint,
	texto text,
	pagenumber integer,
	pagesize integer,
	sortfield varchar,
	sortorder varchar
);
--
CREATE OR REPLACE FUNCTION public.up_solicitudsunat_search_by (
	IN culture varchar,
	IN empresaid bigint,
	IN estadobusquedaid smallint,
	IN texto text,
	IN pagenumber integer  = 0,
	IN pagesize integer  = 0,
	IN sortfield varchar  = ''::character varying,
	IN sortorder varchar  = ''::character varying
)
RETURNS TABLE 
(
	id bigint,
	empresa_id bigint,
	tipo_id bigint,
	tipo_nombre text,
	tipo_codigo varchar,
	tipo_valor varchar,
	tipo_clasificacion varchar,
	tipo_atributo text,
	fecha_solicitud text,
	fecha_solicitud_orden text,
	estado smallint,
	estado_nombre text,
	fecha_envio timestamp WITH TIME ZONE,
	codigo_respuesta varchar,
	descripcion_respuesta text,
	respuesta text,
	fecha_respuesta text,
	fecha_respuesta_orden text,
	fecha_emision varchar,
	fecha_emision_orden text,
	numeracion varchar,
	tiene_xml smallint,
	tiene_cdr smallint
)
AS
$$
DECLARE 
	strsql text;
	limitValue integer = 0;
	offsetValue integer = 0;
BEGIN
	IF (empresaId IS NULL OR LENGTH(TRIM(empresaId::varchar)) = 0) THEN
		empresaId := 0;
	END IF;
	IF (estadoBusquedaId IS NULL OR LENGTH(TRIM(estadoBusquedaId::varchar)) = 0) THEN
		estadoBusquedaId := null;
	END IF;
	IF (texto IS NULL OR LENGTH(TRIM(texto)) = 0) THEN
		texto := '';
	END IF;
	IF (pageNumber IS NULL OR LENGTH(TRIM(pageNumber::varchar)) = 0) THEN
		pageNumber := 0;
	END IF;
	IF (pageSize IS NULL OR LENGTH(TRIM(pageSize::varchar)) = 0) THEN
		pageSize := 0;
	END IF;
	IF (pageNumber < 1) THEN
			pageNumber := 1;
	END IF;
	IF (sortField IS NULL OR LENGTH(TRIM(sortField)) = 0) THEN
		sortField := '';
	END IF;
	IF (sortOrder IS NULL OR LENGTH(TRIM(sortOrder)) = 0) THEN
		sortOrder := '';
	END IF;

	limitValue := pageSize;
	offsetValue := (pageNumber - 1) * pageSize;

	strsql := '
		WITH idm AS (
			SELECT * FROM sbssys.up_idiomacontenido_traer_in_culture($4)
		),
		est AS (
			SELECT 
				idm.id, idm.valor,
				CASE WHEN idm.id = 211 THEN ''1''
						 WHEN idm.id = 212 THEN ''2''
						 WHEN idm.id = 213 THEN ''3,4''
						 WHEN idm.id = 214 THEN ''5,6''
				END AS estados,
				CASE WHEN idm.id = 211 THEN 1
						 WHEN idm.id = 212 THEN 1
						 WHEN idm.id = 213 THEN 2
						 WHEN idm.id = 214 THEN 0
				END::smallint AS estado_busqueda
			FROM idm 
			WHERE idm.id IN (211,212,213,214)
		)

		SELECT DISTINCT
			slct.id,
			slct.empresa_id,
			tp_slct.id AS tipo_id,
			COALESCE(tp_nm.valor, '''') AS tipo_nombre,
			tp_slct.codigo AS tipo_codigo,
			tp_slct.valor AS tipo_valor,
			tp_slct.clasificacion AS tipo_clasificacion,
			tp_slct.atributo AS tipo_atributo,
			TO_CHAR(slct.fecha_creacion,''YYYY-MM-DD'') AS fecha_solicitud,
			TO_CHAR(slct.fecha_creacion,''YYYYMMDD'') AS fecha_solicitud_orden,
			slct.estado,
			est.valor AS estado_nombre,
			slct.fecha_envio,
			slct.codigo_respuesta,
			slct.descripcion_respuesta,
			COALESCE(slct.codigo_respuesta, '''') || COALESCE('' - '' || slct.descripcion_respuesta,'''') AS respuesta,
			TO_CHAR(slct.fecha_respuesta,''YYYY-MM-DD'') AS fecha_respuesta,
			TO_CHAR(slct.fecha_respuesta,''YYYYMMDD'') AS fecha_respuesta_orden,
			COALESCE(dc.fecha_emision, rs.fecha_emision, bj.fecha_emision, '''') AS fecha_emision,
			REPLACE(COALESCE(dc.fecha_emision, rs.fecha_emision, bj.fecha_emision), ''-'', '''') AS fecha_emision_orden,
			COALESCE(
				dc.numeracion,
				(select string_agg(COALESCE(bj_dt.serie,'''') || COALESCE(''-'' || bj_dt.numero, ''''), '','')
				 from "public".bajadocumentodetalle bj_dt WHERE bj_dt.bajadocumento_id = bj.id),
				(select string_agg(COALESCE(rs_dt.serie,'''') || COALESCE(''-'' || rs_dt.numero_inicio, '''') 
													|| '' -> '' || 
													COALESCE(rs_dt.serie,'''') || COALESCE(''-'' || rs_dt.numero_fin, ''''),'''')
				 from "public".resumendocumentodetalle rs_dt WHERE rs_dt.resumendocumento_id = rs.id),NULL
			) AS numeracion,
			(CASE WHEN slct.estado <> 0 AND LENGTH(TRIM(slct.xml)) > 0 
					THEN 1 ELSE 0 
			 END)::smallint AS tiene_xml,
			(CASE WHEN slct.estado IN(3,4) AND LENGTH(TRIM(slct.cdr)) > 0 
					THEN 1 ELSE 0 
			 END)::smallint AS tiene_cdr
		FROM "public".solicitudsunat slct
			INNER JOIN sbssys.enumeracion tp_slct ON tp_slct.id = slct.tiposolicitud_id
			INNER JOIN est ON POSITION('','' || slct.estado::text IN '','' || est.estados) > 0
			LEFT JOIN idm AS tp_nm ON tp_nm.id = tp_slct.nombre_id
			LEFT JOIN "public".documento dc ON dc.solicitudsunat_id = slct."id"
			LEFT JOIN "public".bajadocumento bj ON bj.solicitudsunat_id = slct."id"
			LEFT JOIN "public".resumendocumento rs ON rs.solicitudsunat_id = slct."id"
		WHERE slct.estado <> 0
		AND ($1 = 0 OR slct.empresa_id = $1)
		AND (
			($2 = 99 OR $2 is null) OR est.estado_busqueda = $2
		)
		AND (
			$3 = '''' OR
			(
				POSITION(''-'' || $3 || ''-'' IN COALESCE(TO_CHAR(slct.fecha_creacion,''-YYYY-MM-DD-''),'''')) > 0 OR	
				POSITION(''-'' || $3 || ''-'' IN COALESCE(TO_CHAR(slct.fecha_respuesta,''-YYYY-MM-DD-''),'''')) > 0 OR
				POSITION(''-'' || $3 || ''-'' IN (''-'' || COALESCE(dc.fecha_emision, rs.fecha_emision, bj.fecha_emision, '''') || ''-'')) > 0 OR 
				POSITION(upper($3) IN upper(COALESCE(tp_nm.valor, ''''))) > 0 OR
				POSITION(upper($3) IN upper(COALESCE(slct.codigo_respuesta, ''''))) > 0 OR 
				POSITION(upper($3) IN upper(COALESCE(slct.descripcion_respuesta, ''''))) > 0 OR
				POSITION(upper($3) IN upper(COALESCE(dc.numeracion, ''''))) > 0 OR 
				EXISTS (
					SELECT bj_dt.id from "public".bajadocumentodetalle bj_dt
					WHERE bj_dt.bajadocumento_id = bj.id
					AND POSITION(upper($3) IN upper(COALESCE(bj_dt.serie,'''') || COALESCE(''-'' || bj_dt.numero, ''''))) > 0
				) OR
				EXISTS (
					SELECT rs_dt.id  from "public".resumendocumentodetalle rs_dt
					WHERE rs_dt.resumendocumento_id = rs.id
					AND (
						POSITION(upper($3) IN upper(COALESCE(rs_dt.serie,'''') || COALESCE(''-'' || rs_dt.numero_inicio, ''''))) > 0 OR
						POSITION(upper($3) IN upper(COALESCE(rs_dt.serie,'''') || COALESCE(''-'' || rs_dt.numero_fin, ''''))) > 0
					) 
				)
			)
		)
	';

	IF LENGTH(TRIM(sortField)) > 0 AND LENGTH(TRIM(sortOrder)) > 0 THEN
		strsql := strsql ||
			' ORDER BY ' || sortField || ' ' || sortOrder || ' ';
	END IF;

	IF (limitValue > 0 AND offsetValue > 0) THEN
		strsql := strsql ||
			' LIMIT ' || limitValue || ' OFFSET ' || offsetValue || ' ';
	END IF;

	RETURN query
		EXECUTE strsql USING empresaId, estadoBusquedaId, texto, culture;

END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;