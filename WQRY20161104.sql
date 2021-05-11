DROP FUNCTION public.up_documento_get_prestamos_by_empresa (
	p_query text,
	p_tipomovimientodocumento_id bigint,
	p_empresa_id bigint,
	p_page_number smallint,
	p_page_size smallint
);
--
CREATE OR REPLACE FUNCTION public.up_documento_get_prestamos_by_empresa (
	IN  p_query text,
	IN  p_tipomovimientodocumento_id bigint,
	IN  p_empresa_id bigint,
	IN  p_page_number smallint,
	IN  p_page_size smallint
)
RETURNS TABLE
(
	id bigint,
	persona_id bigint,
	persona_nombre text,
	persona_dni varchar,
	importe_prestado numeric,
	fecha_emision timestamp with time zone,
	cant_cuotas integer,
	importe_total numeric,
	amortizaciones numeric,
	cuotas_pagadas integer,
	saldo_pendiente numeric,
	cuotas_txt text,
	importe_cuota numeric,
	moneda_id bigint,
	moneda_simbolo varchar,
	convenio_id bigint,
	convenio_nombre text  
)
AS
$$
DECLARE
	v_personas_id text := '';
BEGIN
	IF (p_page_number IS NULL OR LENGTH(TRIM(p_page_number::varchar)) = 0) THEN
		p_page_number := 0;
	END IF;

	IF (p_page_size IS NULL OR LENGTH(TRIM(p_page_size::varchar)) = 0) THEN
		p_page_size := 0;
	END IF;

	IF (p_page_number < 1) THEN
		p_page_number := 1;
	END IF;

	IF (p_page_size = 0 OR p_page_number = 0) THEN
		RETURN QUERY
		SELECT DISTINCT
			doc.id,
			prs.id AS persona_id,
			prs.apellidos_nombres AS persona_nombre,
			prs.dni AS persona_dni,
			doc.importe as importe_prestado,
			doc.fecha_emision,
			tblcp.cant_cuotas,
			tblcp.importe_total,
			tblcp.amortizaciones,
			tblcp.cuotas_pagadas,
			tblcp.saldo_pendiente,
			tblcp.cuotas_txt,
			tblcp.importe as importe_cuota,
			mon.id as moneda_id,
			mon.simbolo as moneda_simbolo,
			con.id as convenio_id,
			con.nombre as convenio_nombre
		FROM public.documento doc
		INNER JOIN public.documentocampovalor dcv ON dcv.documento_id = doc.id
		INNER JOIN sbslog.tipomovimientodocumentocampo tmdc ON tmdc.tipomovimientodocumento_id = p_tipomovimientodocumento_id
		INNER JOIN sbspla.cronogramapagos cp ON cp.documento_id = doc.id AND cp.persona_id = dcv.persona_id
		INNER JOIN sbspla.up_cronogramapagos_get_data_by_documento(cp.documento_id, cp.persona_id) as tblcp on TRUE
		INNER JOIN sbspla.planillaperiodo pp ON pp.id = cp.planillaperiodo_id
		INNER JOIN sbspla.planilla pla ON pla.id = pp.planilla_id AND pla.empresa_id = p_empresa_id
		INNER JOIN sbsep.view_persona prs ON prs.id = dcv.persona_id
		INNER JOIN public.moneda mon ON doc.moneda_id = mon.id
		LEFT OUTER JOIN /*FULL OUTER JOIN*/ sbspla.planillaconvenio con ON doc.planillaconvenio_id = con.id
		WHERE doc.estadostd_id = 500
		AND doc.estado = 1
		AND doc.tipomovimientodocumento_id = p_tipomovimientodocumento_id
		AND tmdc.estado = 1
		AND tmdc.es_principal = 1
		AND dcv.campodocumento_id = tmdc.campodocumento_id
		AND (p_query = '' OR p_query IS NULL OR prs.nombre_completo ILIKE '%' || lower(trim(p_query)) || '%' OR prs.dni ILIKE '%' || lower(trim(p_query)) || '%')
		ORDER BY prs.apellidos_nombres;
	ELSE
		RETURN QUERY
		SELECT DISTINCT
			doc.id,
			prs.id AS persona_id,
			prs.apellidos_nombres AS persona_nombre,
			prs.dni AS persona_dni,
			doc.importe as importe_prestado,
			doc.fecha_emision,
			tblcp.cant_cuotas,
			tblcp.importe_total,
			tblcp.amortizaciones,
			tblcp.cuotas_pagadas,
			tblcp.saldo_pendiente,
			tblcp.cuotas_txt,
			tblcp.importe as importe_cuota,
			mon.id as moneda_id,
			mon.simbolo as moneda_simbolo,
			con.id as convenio_id,
			con.nombre as convenio_nombre
		FROM public.documento doc
		INNER JOIN public.documentocampovalor dcv ON dcv.documento_id = doc.id
		INNER JOIN sbslog.tipomovimientodocumentocampo tmdc ON tmdc.tipomovimientodocumento_id = p_tipomovimientodocumento_id
		INNER JOIN sbspla.cronogramapagos cp ON cp.documento_id = doc.id AND cp.persona_id = dcv.persona_id
		INNER JOIN sbspla.up_cronogramapagos_get_data_by_documento(cp.documento_id, cp.persona_id) as tblcp on TRUE
		INNER JOIN sbspla.planillaperiodo pp ON pp.id = cp.planillaperiodo_id
		INNER JOIN sbspla.planilla pla ON pla.id = pp.planilla_id AND pla.empresa_id = p_empresa_id
		INNER JOIN sbsep.view_persona prs ON prs.id = dcv.persona_id
		INNER JOIN public.moneda mon ON doc.moneda_id = mon.id
		LEFT OUTER JOIN /*FULL OUTER JOIN*/ sbspla.planillaconvenio con ON doc.planillaconvenio_id = con.id
		WHERE doc.estadostd_id = 500
		AND doc.estado = 1
		AND doc.tipomovimientodocumento_id = p_tipomovimientodocumento_id
		AND tmdc.estado = 1
		AND tmdc.es_principal = 1
		AND dcv.campodocumento_id = tmdc.campodocumento_id
		AND (p_query = '' OR p_query IS NULL OR prs.nombre_completo ILIKE '%' || lower(trim(p_query)) || '%' OR prs.dni ILIKE '%' || lower(trim(p_query)) || '%')
		ORDER BY prs.apellidos_nombres
		LIMIT p_page_size
		OFFSET ((p_page_number - 1) * p_page_size);
	END IF;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;