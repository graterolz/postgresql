DROP FUNCTION public.up_documento_documentocampovalor_save (
	p_empresa_id bigint,
	p_usuario_id bigint,
	p_serie varchar,
	p_numero varchar,
	p_persona_id bigint,
	p_tipomovimientodocumento_id bigint,
	p_data_documento text,
	p_data_campos text,
	p_conceptodetraccion_id bigint,
	p_lineacreditopersona_id bigint,
	p_usar_credito smallint,
	p_periodopago_id bigint,
	p_periodopago_valor integer
);
--
CREATE OR REPLACE FUNCTION public.up_documento_documentocampovalor_save (
	IN  p_empresa_id bigint,
	IN  p_usuario_id bigint,
	IN  p_serie varchar,
	IN  p_numero varchar,
	IN  p_persona_id bigint,
	IN  p_tipomovimientodocumento_id bigint,
	IN  p_data_documento text,
	IN  p_data_campos text,
	IN  p_conceptodetraccion_id bigint,
	IN  p_lineacreditopersona_id bigint,
	IN  p_usar_credito smallint,
	IN  p_periodopago_id bigint,
	IN  p_periodopago_valor integer
)
RETURNS TABLE 
(
	documento_id bigint
)
AS
$$
DECLARE 
	v_documento_id bigint;
	v_ult_numero varchar;
BEGIN
	IF p_numero IS NULL THEN
		SELECT tbl.numero::VARCHAR INTO v_ult_numero FROM public.up_documento_generar_ultimo_numero(p_tipomovimientodocumento_id,'1') tbl;
	ELSE
		SELECT p_numero INTO v_ult_numero; 
	END IF;

	INSERT INTO public.documento (
		empresa_id,
		tipomovimientodocumento_id,
		serie,
		numero,
		fecha_emision,
		moneda_id,
		importe,
		efectivo,
		pagado,
		liquidado,
		tipo_cambio,
		glosa,
		periodocontable_id,
		organizadordestino_id,
		lineacreditopersona_id,
		lineacreditopersona_usado,
		lineacreditopersona_tipocambio,
		tiene_detraccion,
		valor_detraccion,
		conceptodetraccion_id,
		constantedetraccion_id,
		tiene_percepcion,
		valor_percepcion,
		constantepercepcion_id,
		estado,
		estadostd_id,
		usuariocreador_id,
		planillaconvenio_id
	) 
	SELECT
		p_empresa_id,
		(rec->>'tipomovimientodocumento_id')::BIGINT,
		NULLIF(TRIM(rec->>'serie'),'')::VARCHAR,
		v_ult_numero,
		COALESCE (NULLIF(TRIM(rec->>'fecha_emision'),'')::TIMESTAMP WITH TIME ZONE,CURRENT_TIMESTAMP)::TIMESTAMP WITH TIME ZONE,
		NULLIF(TRIM(rec->>'moneda_id'),'')::BIGINT,
		NULLIF(TRIM(rec->>'importe'),'')::NUMERIC,
		NULLIF(TRIM(rec->>'efectivo'),'')::NUMERIC,
		NULLIF(TRIM(rec->>'pagado'),'')::NUMERIC,
		NULLIF(TRIM(rec->>'liquidado'),'')::NUMERIC,
		NULLIF(TRIM(rec->>'tipo_cambio'),'')::NUMERIC,
		NULLIF(TRIM(rec->>'glosa'),'')::TEXT,
		NULLIF(TRIM(rec->>'periodocontable_id'),'')::BIGINT,
		NULLIF(TRIM(rec->>'organizadordestino_id'),'')::BIGINT,
		NULLIF(TRIM(rec->>'lineacreditopersona_id'),'')::BIGINT,
		NULLIF(TRIM(rec->>'lineacreditopersona_usado'),'')::NUMERIC,
		NULLIF(TRIM(rec->>'lineacreditopersona_tipocambio'),'')::NUMERIC,
		NULLIF(TRIM(rec->>'tiene_detraccion'),'')::SMALLINT,
		CASE WHEN TRIM(rec->>'tiene_detraccion')::SMALLINT = '1' THEN (detraccion.valor * NULLIF(TRIM(rec->>'importe'),'')::NUMERIC)/100::NUMERIC ELSE NULL END,
		CASE WHEN TRIM(rec->>'tiene_detraccion')::SMALLINT = '1' THEN detraccion.id ELSE NULL END,
		CASE WHEN TRIM(rec->>'tiene_detraccion')::SMALLINT = '1' THEN detraccion.constantecontable_id ELSE NULL END,
		NULLIF(TRIM(rec->>'tiene_percepcion'),'')::SMALLINT,
		CASE WHEN TRIM(rec->>'tiene_percepcion')::SMALLINT = '1' THEN (percepcion.valor * NULLIF(TRIM(rec->>'importe'),'')::NUMERIC)/100::NUMERIC ELSE NULL END,
		CASE WHEN TRIM(rec->>'tiene_percepcion')::SMALLINT = '1' THEN percepcion.constantecontable_id ELSE NULL END,
		'1',
		(rec->>'estadostd_id')::BIGINT,
		p_usuario_id,
		NULLIF(TRIM(rec->>'planillaconvenio_id'),'')::BIGINT
	FROM json_array_elements(p_data_documento::json) rec
	LEFT JOIN (
		SELECT cd.id AS "id",cc.id as "constantecontable_id",cc.valor AS "valor"
		FROM sbsctb.conceptodetraccion cd
		INNER JOIN sbsctb.constantecontable cc ON cc.id = cd.constante_id
		WHERE cd.id = NULLIF(TRIM(p_conceptodetraccion_id::TEXT),'')::BIGINT 
		AND cd.estado = 1
	) AS detraccion ON TRUE
	LEFT JOIN (
		SELECT atrib.id AS "id",cc.id as "constantecontable_id",cc.valor AS "valor"
		FROM sbsctb.afectaciontributaria atrib
		INNER JOIN sbsctb.constantecontable cc	ON cc.id = atrib.constante_id
		WHERE atrib.id = 1
		AND atrib.estado = 1
	) AS percepcion ON TRUE
	--
	RETURNING id INTO v_documento_id;
	--
	INSERT INTO public.documentocampovalor (
		documento_id,
		campodocumento_id,
		valor_texto,
		valor_numerico,
		valor_fecha,
		itemlista_id,
		persona_id,
		estado,
		usuariocreador_id,
		vehiculo_id,
		personadireccion_id
	)
	SELECT
		v_documento_id,
		(rec->>'campodocumento_id')::BIGINT,
		CASE WHEN TRIM(rec->>'campodocumento_id') = '25' THEN v_ult_numero::TEXT ELSE NULLIF(TRIM(rec->>'valor_texto'),'')::TEXT END,
		NULLIF(TRIM(rec->>'valor_numerico'),'')::NUMERIC,
		NULLIF(TRIM(rec->>'valor_fecha'),'')::TIMESTAMP WITH TIME ZONE,
		NULLIF(TRIM(rec->>'itemlista_id'),'')::BIGINT,
		CASE WHEN TRIM(rec->>'campodocumento_id') = '4' THEN p_persona_id ELSE NULLIF(TRIM(rec->>'persona_id'),'')::BIGINT END,
		'1',
		p_usuario_id,
		NULLIF(TRIM(rec->>'vehiculo_id'),'')::BIGINT,
		NULLIF(TRIM(rec->>'personadireccion_id'),'')::BIGINT
	FROM json_array_elements(p_data_campos::json) rec;
	--
	UPDATE sbslog.tipomovimientodocumento
	SET ultimo_numero = (
		SELECT COALESCE(tmd.ultimo_numero+1,tmdc.valor_numerico,1)
		FROM sbslog.tipomovimientodocumento tmd
		INNER JOIN sbslog.tipomovimientodocumentocampo tmdc ON tmdc.tipomovimientodocumento_id = tmd.id
		INNER JOIN sbsdoc.campodocumento cd ON cd.id = tmdc.campodocumento_id
		WHERE tmdc.tipomovimientodocumento_id = p_tipomovimientodocumento_id
		AND cd.tipo_id = 454
	)
	WHERE id = p_tipomovimientodocumento_id;
	--
	IF p_usar_credito = '1' THEN
		UPDATE sbsctb.lineacreditopersona
		SET periodopago_id = p_periodopago_id,
			periodopago_valor = p_periodopago_valor
		WHERE id = NULLIF(TRIM(p_lineacreditopersona_id::TEXT),'')::BIGINT;
	END IF;
	--
	RETURN query
	SELECT v_documento_id;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;