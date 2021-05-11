DROP FUNCTION sbsep.up_personas_traer_x_permisos (
	p_query text,
	p_modo text,
	p_grupo_id text,
	p_puesto_id text,
	p_usuario_id bigint,
	p_componenteinstancia_id bigint,
	p_control_id bigint,
	p_tipo_contrato text,
	p_grupos_recursivos smallint,
	p_dias_por_vencer bigint,
	p_culture text
);
--
CREATE OR REPLACE FUNCTION sbsep.up_personas_traer_x_permisos (
	IN	p_query text,
	IN	p_modo text,
	IN	p_grupo_id text,
	IN	p_puesto_id text,
	IN	p_usuario_id bigint,
	IN	p_componenteinstancia_id bigint,
	IN	p_control_id bigint,
	IN	p_tipo_contrato text,
	IN	p_grupos_recursivos smallint,
	IN	p_dias_por_vencer bigint,
	IN	p_culture text,
	IN	p_personasHorario boolean
)
RETURNS	TABLE	
(
	id bigint,
	nombre_completo text,
	estado smallint,
	usuario_estado smallint,
	tipo varchar,
	ambito text,
	dni varchar,
	cusp varchar,
	vacaciones_disponibles integer,
	tiempo_vencimiento_no_prog integer,
	vacaciones_no_programadas integer,
	vacaciones_pendientes_ant integer,
	vacaciones_programadas integer,
	vacaciones_gozadas integer,
	vacaciones_ganadas_vigentes integer,
	vacaciones_gozadas_vigentes integer,
	vacaciones_adiciones_tributarias integer,
	txt_tiempo_vencimiento_no_prog text,
	txt_vacaciones_no_programadas text,
	txt_vacaciones_pendientes_ant text,
	txt_vacaciones_programadas text,
	txt_vacaciones_gozadas text,
	txt_vacaciones_ganadas_vigentes text,
	txt_vacaciones_gozadas_vigentes text,
	txt_vacaciones_adiciones_tributarias text,
	codigo varchar,
	email varchar,
	horario varchar,
	grupo text,
	puesto text,
	telefono varchar,
	termino_contrato timestamp with time zone,
	planilla varchar,
	tiene_contrato boolean,
	usuario_id bigint,
	contrato_id bigint
)
AS
$$
DECLARE	
	v_persona_id bigint;
	v_empresa_id bigint;
	v_contratado boolean;
	v_sin_contrato boolean;
	v_renovado boolean;
	v_por_vencer boolean;
	v_sql_query text;
	v_array_query text[];
	v_query text;
	v_personas_con_permisos_id text;
	v_lbl_dia text;
	v_lbl_dias text;
	v_lbl_mes text;
	v_lbl_meses text;
	v_contratopersonas_id text;
BEGIN
	SELECT string_to_array(p_query,' ') INTO v_array_query;
	v_sql_query := '';
	--
	FOREACH v_query IN ARRAY v_array_query
	LOOP
		IF(trim(v_query) <> '') THEN
			v_sql_query := v_sql_query || '(%' || lower(trim(v_query)) || '%)';
		END	IF;
	END	LOOP;
	--
	v_contratado:=false;
	v_sin_contrato:=false;
	v_renovado:=false;
	v_por_vencer:=false;
	--
	CASE p_tipo_contrato
		WHEN 'CTR' THEN v_contratado:=true;
		WHEN 'SCTR' THEN v_sin_contrato:=true;
		WHEN 'CTRR' THEN v_renovado:=true;
		WHEN 'CTRV' THEN v_por_vencer:=true;
		ELSE NULL	
	END	CASE;
	--
	SELECT md.empresa_id INTO v_empresa_id 
	FROM sbssys.componenteinstancia	ci
	INNER JOIN sbsadm.modulo md	ON md.id = ci.modulo_id
	WHERE ci.id = p_componenteinstancia_id
	LIMIT 1;
	--
	SELECT public.agr_fila_unir(prs.id::TEXT) INTO v_personas_con_permisos_id
	FROM sbsep.up_personas_id_traer_x_permisos (
		p_modo,
		p_usuario_id,
		p_componenteinstancia_id,
		p_control_id,
		v_empresa_id
	) prs;
	--	
	SELECT public.agr_fila_unir_unique(cp.id::TEXT) INTO v_contratopersonas_id
	FROM sbspla.contratoplanilla cp
	WHERE cp.planilla_id IS NOT	NULL
	AND cp.persona_id IN (
		SELECT UNNEST(string_to_array(v_personas_con_permisos_id,','))::BIGINT
	)
	AND now() BETWEEN cp.vigencia_inicio AND cp.vigencia_fin
	AND cp.estado = 1;
	--	
	SELECT lower(valor) INTO v_lbl_dia
	FROM sbssys.up_idiomacontenido_traer_in_culture_x_id (p_culture,1219::BIGINT);
	--
	SELECT lower(valor) INTO v_lbl_dias
	FROM sbssys.up_idiomacontenido_traer_in_culture_x_id (p_culture,2313::BIGINT);
	--
	SELECT lower(valor)	INTO v_lbl_mes
	FROM sbssys.up_idiomacontenido_traer_in_culture_x_id (p_culture,1944::BIGINT);
	--
	SELECT lower(valor) INTO v_lbl_meses
	FROM sbssys.up_idiomacontenido_traer_in_culture_x_id(p_culture,3938::BIGINT);
	--
	RAISE NOTICE '=> % | % | % | % | %',v_empresa_id,p_grupo_id,p_puesto_id,p_grupos_recursivos,v_contratopersonas_id;
	--
	RETURN QUERY
	SELECT DISTINCT
		prs.id,
		prs.apellidos_nombres,
		prs.estado,
		usr.estado,
		tipoPrs.nombre,
		prsAmb.persona_ambito,
		prs.dni,
		prs.cuspp,
		vac.vacaciones_disponibles,
		vac.tiempo_vencimiento_no_prog,
		vac.vacaciones_no_programadas,
		vac.vacaciones_pendientes_anteriores,
		vac.vacaciones_programadas::INTEGER,
		vac.vacaciones_gozadas,
		vac.vacaciones_ganadas_vigentes,
		vac.vacaciones_gozadas_vigentes,
		vac.vacaciones_adiciones_tributarias,
		COALESCE(vac.vacaciones_formatted_tiempo_venc,'--')	AS txt_tiempo_vencimiento_no_prog,
		COALESCE(
			CASE
				WHEN vac.vacaciones_no_programadas = 1 THEN '1 ' || v_lbl_dia
				ELSE vac.vacaciones_no_programadas::TEXT || ' ' || v_lbl_dias
			END,'--'
		)AS	txt_vacaciones_no_programadas,
		COALESCE(
			CASE
				WHEN vac.vacaciones_pendientes_anteriores = 1 THEN '1 ' || v_lbl_dia
				ELSE vac.vacaciones_pendientes_anteriores::TEXT || ' días'
			END,'--'
		) AS txt_vacaciones_pendientes_anteriores,
		COALESCE(
			CASE
				WHEN vac.vacaciones_programadas = 1 THEN '1 ' || v_lbl_dia
				ELSE vac.vacaciones_programadas::TEXT || ' ' || v_lbl_dias
			END,'--'
		) AS txt_vacaciones_programadas,
		COALESCE(
			CASE
				WHEN vac.vacaciones_gozadas = 1 THEN '1 ' || v_lbl_dia
				ELSE vac.vacaciones_gozadas::TEXT || ' ' || v_lbl_dias
			END,'--'
		) AS txt_vacaciones_gozadas,
		COALESCE(
			CASE
				WHEN vac.vacaciones_ganadas_vigentes = 1 THEN '1 ' || v_lbl_dia
				ELSE vac.vacaciones_ganadas_vigentes::TEXT || ' ' || v_lbl_dias
			END,'--'
		) AS txt_vacaciones_ganadas_vigentes,
		COALESCE(
			CASE
				WHEN vac.vacaciones_gozadas_vigentes = 1 THEN '1 ' || v_lbl_dia
				ELSE vac.vacaciones_gozadas_vigentes::TEXT || ' ' || v_lbl_dias
			END,'--'
		) AS txt_vacaciones_gozadas_vigentes,
		COALESCE(vac.vacaciones_adiciones_tributarias::TEXT,'--') AS txt_vacaciones_adiciones_tributarias,
		prs.codigo,
		prs.email,
		phv.horario_nombre,
		hora.nombre,
		gpp.grupo_nombre,
		gpp.puesto_nombre,
		prs.telefono_sms,
		cp.vigencia_fin,
		pla.nombre::varchar,
		COALESCE(prs.id = cp.persona_id AND cp.termino_contrato_motivo_id IS NULL,false),
		usr.id,
		cp.id
	FROM sbsep.view_persona prs	
	INNER JOIN sbscrm.up_personas_grupos_puestos(
		v_empresa_id,p_grupo_id,p_puesto_id,p_grupos_recursivos,p_culture
	) gpp ON gpp.persona_id = prs.id
	LEFT JOIN sbscrm.vw_personas_ambito_fv_cta prsAmb ON prsAmb.persona_id = prs.id	
	LEFT JOIN sbspla.contratoplanilla cp ON cp.persona_id = prs.id AND cp.estado = 1 AND (
		(cp.vigencia_fin IS NOT NULL AND 
		now() BETWEEN cp.vigencia_inicio AND cp.vigencia_fin) 
		OR
		(cp.vigencia_fin IS NULL AND now() >= cp.vigencia_inicio)
	)
	LEFT JOIN (
		SELECT DISTINCT
			cp.persona_id,first_value(cp.id) OVER (PARTITION BY cp.persona_id ORDER BY cp.vigencia_inicio DESC) AS contrato_id
		FROM sbspla.contratoplanilla cp
		WHERE cp.estado = 1
		AND cp.vigencia_inicio < now()
		AND cp.persona_id IN (SELECT UNNEST(string_to_array(v_personas_con_permisos_id,	','))::BIGINT)
	) dtContrato ON dtContrato.persona_id = prs.id
	LEFT JOIN sbspla.contratoplanilla cp ON cp.id = dtContrato.contrato_id
	LEFT JOIN (
		SELECT	DISTINCT
			persona_id,	first_value(pper.planilla_id) OVER (PARTITION BY pper.persona_id ORDER BY pper.fecha_inicio DESC) AS planilla_id
		FROM sbspla.planillapersona	pper
		WHERE pper.estado = 1
		AND pper.fecha_inicio < now()
		AND pper.persona_id	IN (SELECT UNNEST(string_to_array(v_personas_con_permisos_id,','))::BIGINT)
	) dtPlanilla ON dtPlanilla.persona_id = prs.id
	LEFT JOIN sbspla.planilla pla ON pla.id = dtPlanilla.planilla_id
	LEFT JOIN sbsep.tipopersona	tipoPrs	ON tipoPrs.id = prs.tipopersona_id
	LEFT JOIN sbsadm.usuario usr ON usr.persona_id = prs.id AND usr.estado <> 0
	LEFT JOIN (
		SELECT DISTINCT
			ph.persona_id, first_value(ph.horario_id) OVER (PARTITION BY ph.persona_id ORDER BY ph.vigencia_inicio DESC) AS horario_id
		FROM sbstar.personahorario ph
		WHERE ph.estado = 1	
		AND ph.vigencia_inicio < now()
		AND ph.persona_id IN (SELECT UNNEST(string_to_array(v_personas_con_permisos_id,','))::BIGINT)
	) dtHorario ON dtHorario.persona_id = prs.id
	LEFT JOIN sbstar.horario hora ON hora.id = dtHorario.horario_id
	LEFT JOIN sbscrm.up_personas_contratos_estado(p_dias_por_vencer) pct ON pct.persona_id = prs.id
	LEFT JOIN sbstar.up_configuracionexcepcion_bolsatiempo_vacaciones_by_contrato(
		v_contratopersonas_id,now(),v_lbl_dia,v_lbl_dias,v_lbl_mes,v_lbl_meses
	) vac ON vac.persona_id = prs.id
	WHERE prs.estado <> 0
	AND prs.tipo_id = 218
	AND	(
		lower(prs.nombre_completo) SIMILAR TO v_sql_query OR
		tipoPrs.nombre ILIKE '%' || p_query || '%' OR
		prsAmb.persona_ambito ILIKE '%' || p_query || '%' OR
		prs.dni	ILIKE '%' || p_query || '%' OR
		prs.codigo ILIKE '%' || p_query || '%' OR
		prs.email ILIKE '%' || p_query || '%' OR
		prs.telefono_sms ILIKE '%' || p_query || '%' OR
		hora.nombre	ILIKE '%' || p_query || '%' OR
		gpp.grupo_nombre ILIKE '%' || p_query || '%' OR
		gpp.puesto_nombre ILIKE '%' || p_query || '%'
	)
	AND	(
		(v_contratado=false AND v_sin_contrato=false AND v_renovado=false AND v_por_vencer=false) OR
		(v_contratado=true AND pct.contratado=v_contratado) OR
		(v_sin_contrato=true AND pct.sin_contrato=v_sin_contrato) OR
		(v_renovado=true AND pct.renovado=v_renovado) OR
		(v_por_vencer=true AND pct.por_vencer=v_por_vencer)
	)
	AND	prs.id IN (SELECT UNNEST(string_to_array(v_personas_con_permisos_id,','))::BIGINT)
	AND	(
		(p_personasHorario=true AND COALESCE(hora.nombre,0::VARCHAR) ='0') OR
		(p_personasHorario=false
	);
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;
--
DROP FUNCTION sbstar.up_agenda_traer_base_con_tiempos_procesados_y_excp_base(
	p_json_excepciones text,
	p_age_inicio_planificado timestamp WITH TIME ZONE,
	p_age_fin_planificado timestamp WITH TIME ZONE,
	p_tiempo_planificado integer,
	fechas_excepciones_contratmplab_json text
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_traer_base_con_tiempos_procesados_y_excp_base (
	IN p_json_excepciones text,
	IN p_age_inicio_planificado timestamp WITH TIME ZONE,
	IN p_age_fin_planificado timestamp WITH TIME ZONE,
	IN p_tiempo_planificado integer,
	IN fechas_excepciones_contratmplab_json text
)
RETURNS TABLE 
(
	tiene_excepciones boolean,
	tiempo_excepciones integer,
	fechas_excepciones timestamp WITH TIME ZONE[],
	fechas_excepciones_json text,
	tiempo_contratmplab_exc integer
)
AS
$$
DECLARE
	rd_exc RECORD;
	exc_fecha_inicio TIMESTAMP WITH TIME ZONE;
	exc_fecha_fin TIMESTAMP WITH TIME ZONE;
	tiempo_excepcion INTEGER;
	v_tiene_excepciones BOOLEAN;
	v_tiempo_excepciones INTEGER;
	v_fechas_excepciones TIMESTAMP WITH TIME ZONE[][];
	v_fechas_excepciones_json TEXT;
	v_tiempo_contratmplab_exc INTEGER;	
BEGIN
	exc_fecha_inicio := null;
	exc_fecha_fin := null;
	v_tiene_excepciones := false;
	v_tiempo_excepciones := 0;
	v_fechas_excepciones := null;
	v_fechas_excepciones_json := '';
	--
	FOR rd_exc IN
		SELECT
			(rec->>'inicio')::TIMESTAMP WITH TIME ZONE as fecha_hora_inicio,
			(rec->>'fin')::TIMESTAMP WITH TIME ZONE as fecha_hora_fin
		FROM json_array_elements(p_json_excepciones::json) rec
	LOOP
		CONTINUE WHEN (v_tiempo_excepciones >= p_tiempo_planificado);
		v_tiene_excepciones := true;
		--
		IF (rd_exc.fecha_hora_inicio > exc_fecha_fin) THEN
			tiempo_excepcion := EXTRACT(epoch from exc_fecha_fin - exc_fecha_inicio)::integer;
			v_tiempo_excepciones := v_tiempo_excepciones + tiempo_excepcion;
			v_fechas_excepciones := v_fechas_excepciones || ARRAY[ARRAY[exc_fecha_inicio,exc_fecha_fin]];
			--
			IF v_fechas_excepciones_json <> '' THEN
				v_fechas_excepciones_json := v_fechas_excepciones_json || ',';
			END IF;
			--
			v_fechas_excepciones_json := v_fechas_excepciones_json ||  '{"inicio":"' || exc_fecha_inicio || '","fin":"' || exc_fecha_fin || '"}';
			--
			exc_fecha_inicio := null;
			exc_fecha_fin := null;
			CONTINUE WHEN (v_tiempo_excepciones >= p_tiempo_planificado);
		END IF;
		--
		IF (exc_fecha_inicio IS NULL) THEN
			exc_fecha_inicio := rd_exc.fecha_hora_inicio;
		END IF;
		--
		IF (exc_fecha_fin IS NULL) THEN
			exc_fecha_fin := rd_exc.fecha_hora_fin;
		END IF;
		--
		IF (rd_exc.fecha_hora_inicio < exc_fecha_inicio) THEN
			exc_fecha_inicio := rd_exc.fecha_hora_inicio;
		END IF;
		--
		IF (exc_fecha_inicio < p_age_inicio_planificado) THEN
			exc_fecha_inicio := p_age_inicio_planificado;
		END IF;
		--
		IF (rd_exc.fecha_hora_fin > exc_fecha_fin) THEN
			exc_fecha_fin := rd_exc.fecha_hora_fin;
		END IF;
		--
		IF (exc_fecha_fin > p_age_fin_planificado) THEN
			exc_fecha_fin := p_age_fin_planificado;
		END IF;
		--
		tiempo_excepcion := EXTRACT(epoch from exc_fecha_fin - exc_fecha_inicio)::integer;
		--
		IF ((v_tiempo_excepciones + tiempo_excepcion) >= p_tiempo_planificado) THEN
			v_tiempo_excepciones := v_tiempo_excepciones + tiempo_excepcion;
			v_fechas_excepciones := v_fechas_excepciones || ARRAY[ARRAY[exc_fecha_inicio,exc_fecha_fin]];
			--
			IF v_fechas_excepciones_json <> '' THEN
				v_fechas_excepciones_json := v_fechas_excepciones_json || ',';
			END IF;
			--
			v_fechas_excepciones_json := v_fechas_excepciones_json ||
				'{"inicio":"' || exc_fecha_inicio || '","fin":"' || exc_fecha_fin || '"}';
			--
			exc_fecha_inicio := null;
			exc_fecha_fin := null;
		END IF;
		--
		CONTINUE WHEN (v_tiempo_excepciones >= p_tiempo_planificado);
	END LOOP;
	--
	IF (exc_fecha_fin IS NOT NULL AND exc_fecha_inicio IS NOT NULL) THEN
		v_tiempo_excepciones := v_tiempo_excepciones + EXTRACT(epoch from exc_fecha_fin - exc_fecha_inicio)::integer;
		v_fechas_excepciones := v_fechas_excepciones || ARRAY[ARRAY[exc_fecha_inicio,exc_fecha_fin]];
		--
		IF v_fechas_excepciones_json <> '' THEN
			v_fechas_excepciones_json := v_fechas_excepciones_json || ',';
		END IF;
		--
		v_fechas_excepciones_json := v_fechas_excepciones_json ||
			'{"inicio":"' || exc_fecha_inicio || '","fin":"' || exc_fecha_fin || '"}';
	END IF;
	--
	v_fechas_excepciones_json := '[' || v_fechas_excepciones_json || ']';
	--
	IF fechas_excepciones_contratmplab_json <> '' THEN
		SELECT
			SUM(EXTRACT(epoch from 
				LEAST((jsonTiempoLab->>'fin')::TIMESTAMP WITH TIME ZONE,
				(jsonExc->>'fin')::TIMESTAMP WITH TIME ZONE) - GREATEST((jsonTiempoLab->>'inicio')::TIMESTAMP WITH TIME ZONE,
				(jsonExc->>'inicio')::TIMESTAMP WITH TIME ZONE)	
			)::INTEGER) AS tiempo_contratmplab_goze
		INTO v_tiempo_contratmplab_exc
		FROM json_array_elements(v_fechas_excepciones_json::json) jsonExc
		INNER JOIN json_array_elements(
			fechas_excepciones_contratmplab_json::json
		) jsonTiempoLab ON (
			(jsonTiempoLab->>'inicio')::TIMESTAMP WITH TIME ZONE,
			(jsonTiempoLab->>'fin')::TIMESTAMP WITH TIME ZONE
		) OVERLAPS;
	END IF;
	--
	RETURN query
	SELECT
		v_tiene_excepciones,
		v_tiempo_excepciones,
		v_fechas_excepciones,
		v_fechas_excepciones_json,
		v_tiempo_contratmplab_exc;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;
--
DROP FUNCTION sbstar.up_agendas_save_tiemposcalculados(
	p_agenda_id text,
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_horario_id bigint,
	p_intervalo_id text,
	p_grupo_id text,
	p_persona_id text,
	p_excluir_personas_id text,
	p_usuario_id bigint
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agendas_save_tiemposcalculados (
	IN p_agenda_id text,
	IN p_fecha_inicio timestamp WITH TIME ZONE,
	IN p_fecha_fin timestamp WITH TIME ZONE,
	IN p_horario_id bigint,
	IN p_intervalo_id text,
	IN p_grupo_id text,
	IN p_persona_id text,
	IN p_excluir_personas_id text,
	IN p_usuario_id bigint                  
)
RETURNS boolean AS
$$
BEGIN
	IF NULLIF(TRIM(p_agenda_id),'') IS NOT NULL THEN
		UPDATE sbstar.agenda AS age
		SET teorico = tbl.teorico,
			usado_total = tbl.usado_total,
			usado_en_teorico = tbl.usado_en_teorico,
			distribuir = tbl.distribuir,
			reintegrar = tbl.reintegrar,
			por_compensar = tbl.reintegrar,
			pago_personalizado = tbl.pago_personalizado,
			excepciones_vacaciones = tbl.excepciones_vacaciones,
			exc_vac_prog_noindemnizadas = tbl.excepciones_vacaciones_prog_indemnizadas,
			exc_vac_prog_indemnizadas = tbl.excepciones_vacaciones_prog_indemnizadas,
			excepciones_subsidio_maternidad = tbl.excepciones_subsidio_maternidad,
			excepciones_subsidio_incapacida =  tbl.excepciones_subsidio_incapacidad,
			exc_licencia_goze_lactancia = tbl.excepciones_licencia_goze_lactancia,
			exc_licencia_goze_no_sistema = tbl.excepciones_licencia_goze_no_sistema,
			excepciones_con_goze = tbl.excepciones_con_goze,
			excepciones_feriado = tbl.excepciones_feriado,
			tardanza = tbl.tardanza,
			salida_anticipada = tbl.salida_anticipada,
			horas_nocturnas_total = tbl.horas_nocturnas_total,
			horas_extras_total = tbl.horas_extras_total,
			he_total_en_laborable = tbl.horas_extras_total_en_laborable,
			he_total_en_descanso = tbl.horas_extras_total_en_descanso,
			he_total_en_feriado =  tbl.horas_extras_total_en_feriado,
			he_total_en_feriado_laborable =  tbl.horas_extras_total_en_feriado_laborable,
			he_total_en_feriado_descanso =  tbl.horas_extras_total_en_feriado_descanso,
			tardanza_en_laborable =  tbl.tardanza_en_laborable,
			tardanza_en_descanso = tbl.tardanza_en_descanso,
			salida_anticipada_en_laborable = tbl.salida_anticipada_en_laborable,
			salida_anticipada_en_descanso = tbl.salida_anticipada_en_descanso,
			laborado_teorico = tbl.laborado_teorico,
			laborado_real = tbl.laborado_real,
			laborado_en_teorico = tbl.laborado_en_teorico,
			laborado =  tbl.laborado,
			laborado_total =  tbl.laborado_total,
			descanso_teorico =  tbl.descanso_teorico,
			desc_teo_con_marcaciones =  tbl.descanso_teorico_con_marcaciones,
			laborado_en_descanso =  tbl.laborado_en_descanso,
			laborado_real_en_descanso =  tbl.laborado_real_en_descanso,
			inasistencia_en_laborable =  tbl.inasistencia_en_laborable,
			feriado_teorico =  tbl.feriado_teorico,
			exc_fer_en_descanso =  tbl.excepciones_feriado_en_descanso,
			exc_fer_en_laborable = tbl.excepciones_feriado_en_laborable ,
			laborado_feriado_en_descanso = tbl.laborado_feriado_en_descanso,
			laborado_feriado_en_laborable = tbl.laborado_feriado_en_laborable,
			laborado_feriado =  tbl.laborado_feriado,
			fer_teo_en_desc_con_marcaciones = tbl.feriado_teorico_en_descanso_con_marcaciones,
			fer_teo_en_lab_con_marcaciones =  tbl.feriado_teorico_en_laborable_con_marcaciones,
			fer_teo_con_marcaciones =  tbl.feriado_teorico_con_marcaciones,
			inconsistencias =  tbl.inconsistencias,
			inconsistencias_en_laborable =  tbl.inconsistencias_en_laborable,
			inconsistencias_en_descanso =  tbl.inconsistencias_en_descanso,
			primerodemayo_remunerable =  tbl.primerodemayo_remunerable,
			horariodia_id = tbl.horariodia_id,
			horariodia_inicio_dia = tbl.horariodia_inicio_dia,
			tiene_marcaciones = CASE WHEN tbl.tiene_marcaciones IS TRUE THEN 1::SMALLINT ELSE 0::SMALLINT END,
			total_marcaciones = tbl.total_marcaciones,
			horas_extras_25_en_laborable = tbl.horas_extras_25_en_laborable,
			horas_extras_35_en_laborable = tbl.horas_extras_35_en_laborable,
			excepciones_sin_goze = tbl.excepciones_sin_goze,
			excepciones_json = tbl.excepciones_json
		FROM (
			SELECT DISTINCT
				age.agenda_id::BIGINT,
				tbl.teorico,
				tbl.usado_total,
				tbl.usado_en_teorico,
				tbl.distribuir,
				tbl.reintegrar,
				tbl.pago_personalizado,
				tbl.excepciones_vacaciones,
				tbl.excepciones_vacaciones_prog_noindemnizadas,
				tbl.excepciones_vacaciones_prog_indemnizadas,
				tbl.excepciones_subsidio_maternidad,
				tbl.excepciones_subsidio_incapacidad,
				tbl.excepciones_licencia_goze_lactancia,
				tbl.excepciones_licencia_goze_no_sistema,
				tbl.excepciones_con_goze,
				tbl.excepciones_feriado,
				tbl.tardanza,
				tbl.salida_anticipada,
				tbl.horas_nocturnas_total,
				tbl.horas_extras_total,
				tbl.horas_extras_total_en_laborable,
				tbl.horas_extras_total_en_descanso,
				tbl.horas_extras_total_en_feriado,
				tbl.horas_extras_total_en_feriado_laborable,
				tbl.horas_extras_total_en_feriado_descanso,
				tbl.tardanza_en_laborable,
				tbl.tardanza_en_descanso,
				tbl.salida_anticipada_en_laborable,
				tbl.salida_anticipada_en_descanso,
				tbl.laborado_teorico,
				tbl.laborado_real,
				tbl.laborado_en_teorico,
				tbl.laborado,
				tbl.laborado_total,
				tbl.descanso_teorico,
				tbl.descanso_teorico_con_marcaciones,
				tbl.laborado_en_descanso,
				tbl.laborado_real_en_descanso,
				tbl.inasistencia_en_laborable,
				tbl.feriado_teorico,
				tbl.excepciones_feriado_en_descanso,
				tbl.excepciones_feriado_en_laborable,
				tbl.laborado_feriado_en_descanso,
				tbl.laborado_feriado_en_laborable,
				tbl.laborado_feriado,
				tbl.feriado_teorico_en_descanso_con_marcaciones,
				tbl.feriado_teorico_en_laborable_con_marcaciones,
				tbl.feriado_teorico_con_marcaciones,
				tbl.inconsistencias,
				tbl.inconsistencias_en_laborable,
				tbl.inconsistencias_en_descanso,
				tbl.primerodemayo_remunerable,
				tbl.tiene_marcaciones,
				tbl.total_marcaciones,
				tbl.horas_extras_25_en_laborable,
				tbl.horas_extras_35_en_laborable,
				tbl.horariodia_id,
				tbl.horariodia_inicio_dia,
				tbl.excepciones_sin_goze,
				tbl.excepciones_json
			FROM sbstar.up_agendas_intervalo_traer_data_by_id(p_agenda_id) age
			INNER JOIN sbstar.up_agenda_calcular_tiempos_to_save(
				age.fecha_inicio,
				age.fecha_fin,
				age.horario_id,
				age.intervalo_id::TEXT,
				null::TEXT,
				age.persona_id::TEXT
			) tbl ON TRUE
		) tbl
		WHERE age.id = tbl.agenda_id;
		RETURN TRUE;
	ELSE
		UPDATE sbstar.agenda AS age
		SET teorico = tbl.teorico,
			usado_total = tbl.usado_total,
			usado_en_teorico = tbl.usado_en_teorico,
			distribuir = tbl.distribuir,
			reintegrar = tbl.reintegrar,
			por_compensar = tbl.reintegrar,
			pago_personalizado = tbl.pago_personalizado,
			excepciones_vacaciones = tbl.excepciones_vacaciones,
			exc_vac_prog_noindemnizadas = tbl.excepciones_vacaciones_prog_indemnizadas,
			exc_vac_prog_indemnizadas = tbl.excepciones_vacaciones_prog_indemnizadas,
			excepciones_subsidio_maternidad = tbl.excepciones_subsidio_maternidad,
			excepciones_subsidio_incapacida =  tbl.excepciones_subsidio_incapacidad,
			exc_licencia_goze_lactancia = tbl.excepciones_licencia_goze_lactancia,
			exc_licencia_goze_no_sistema = tbl.excepciones_licencia_goze_no_sistema,
			excepciones_con_goze = tbl.excepciones_con_goze,
			excepciones_feriado = tbl.excepciones_feriado,
			tardanza = tbl.tardanza,
			salida_anticipada = tbl.salida_anticipada,
			horas_nocturnas_total = tbl.horas_nocturnas_total,
			horas_extras_total = tbl.horas_extras_total,
			he_total_en_laborable = tbl.horas_extras_total_en_laborable,
			he_total_en_descanso = tbl.horas_extras_total_en_descanso,
			he_total_en_feriado =  tbl.horas_extras_total_en_feriado,
			he_total_en_feriado_laborable =  tbl.horas_extras_total_en_feriado_laborable,
			he_total_en_feriado_descanso =  tbl.horas_extras_total_en_feriado_descanso,
			tardanza_en_laborable =  tbl.tardanza_en_laborable,
			tardanza_en_descanso = tbl.tardanza_en_descanso,
			salida_anticipada_en_laborable = tbl.salida_anticipada_en_laborable,
			salida_anticipada_en_descanso = tbl.salida_anticipada_en_descanso,
			laborado_teorico = tbl.laborado_teorico,
			laborado_real = tbl.laborado_real,
			laborado_en_teorico = tbl.laborado_en_teorico,
			laborado =  tbl.laborado,
			laborado_total =  tbl.laborado_total,
			descanso_teorico =  tbl.descanso_teorico,
			desc_teo_con_marcaciones =  tbl.descanso_teorico_con_marcaciones,
			laborado_en_descanso =  tbl.laborado_en_descanso,
			laborado_real_en_descanso =  tbl.laborado_real_en_descanso,
			inasistencia_en_laborable =  tbl.inasistencia_en_laborable,
			feriado_teorico =  tbl.feriado_teorico,
			exc_fer_en_descanso =  tbl.excepciones_feriado_en_descanso,
			exc_fer_en_laborable = tbl.excepciones_feriado_en_laborable ,
			laborado_feriado_en_descanso = tbl.laborado_feriado_en_descanso,
			laborado_feriado_en_laborable = tbl.laborado_feriado_en_laborable,
			laborado_feriado =  tbl.laborado_feriado,
			fer_teo_en_desc_con_marcaciones = tbl.feriado_teorico_en_descanso_con_marcaciones,
			fer_teo_en_lab_con_marcaciones =  tbl.feriado_teorico_en_laborable_con_marcaciones,
			fer_teo_con_marcaciones =  tbl.feriado_teorico_con_marcaciones,
			inconsistencias =  tbl.inconsistencias,
			inconsistencias_en_laborable =  tbl.inconsistencias_en_laborable,
			inconsistencias_en_descanso =  tbl.inconsistencias_en_descanso,
			primerodemayo_remunerable =  tbl.primerodemayo_remunerable,
			horariodia_id = tbl.horariodia_id,
			horariodia_inicio_dia = tbl.horariodia_inicio_dia,
			tiene_marcaciones = CASE WHEN tbl.tiene_marcaciones IS TRUE THEN 1::SMALLINT ELSE 0::SMALLINT END,
			total_marcaciones = tbl.total_marcaciones,
			horas_extras_25_en_laborable = tbl.horas_extras_25_en_laborable,
			horas_extras_35_en_laborable = tbl.horas_extras_35_en_laborable,
			excepciones_sin_goze = tbl.excepciones_sin_goze,
			excepciones_json = tbl.excepciones_json
		FROM (
			SELECT
				tbl.agenda_id,
				tbl.teorico,
				tbl.usado_total,
				tbl.usado_en_teorico,
				tbl.distribuir,
				tbl.reintegrar,
				tbl.pago_personalizado,
				tbl.excepciones_vacaciones,
				tbl.excepciones_vacaciones_prog_noindemnizadas,
				tbl.excepciones_vacaciones_prog_indemnizadas,
				tbl.excepciones_subsidio_maternidad,
				tbl.excepciones_subsidio_incapacidad,
				tbl.excepciones_licencia_goze_lactancia,
				tbl.excepciones_licencia_goze_no_sistema,
				tbl.excepciones_con_goze,
				tbl.excepciones_feriado,
				tbl.tardanza,
				tbl.salida_anticipada,
				tbl.horas_nocturnas_total,
				tbl.horas_extras_total,
				tbl.horas_extras_total_en_laborable,
				tbl.horas_extras_total_en_descanso,
				tbl.horas_extras_total_en_feriado,
				tbl.horas_extras_total_en_feriado_laborable,
				tbl.horas_extras_total_en_feriado_descanso,
				tbl.tardanza_en_laborable,
				tbl.tardanza_en_descanso,
				tbl.salida_anticipada_en_laborable,
				tbl.salida_anticipada_en_descanso,
				tbl.laborado_teorico,
				tbl.laborado_real,
				tbl.laborado_en_teorico,
				tbl.laborado,
				tbl.laborado_total,
				tbl.descanso_teorico,
				tbl.descanso_teorico_con_marcaciones,
				tbl.laborado_en_descanso,
				tbl.laborado_real_en_descanso,
				tbl.inasistencia_en_laborable,
				tbl.feriado_teorico,
				tbl.excepciones_feriado_en_descanso,
				tbl.excepciones_feriado_en_laborable,
				tbl.laborado_feriado_en_descanso,
				tbl.laborado_feriado_en_laborable,
				tbl.laborado_feriado,
				tbl.feriado_teorico_en_descanso_con_marcaciones,
				tbl.feriado_teorico_en_laborable_con_marcaciones,
				tbl.feriado_teorico_con_marcaciones,
				tbl.inconsistencias,
				tbl.inconsistencias_en_laborable,
				tbl.inconsistencias_en_descanso,
				tbl.primerodemayo_remunerable,
				tbl.persona_id,
				tbl.tiene_marcaciones,
				tbl.total_marcaciones,
				tbl.horas_extras_25_en_laborable,
				tbl.horas_extras_35_en_laborable,
				tbl.horariodia_id,
				tbl.horariodia_inicio_dia,
				tbl.excepciones_sin_goze,
				tbl.excepciones_json
			FROM sbstar.up_agenda_calcular_tiempos_to_save(
				p_fecha_inicio,
				p_fecha_fin,
				p_horario_id,
				p_intervalo_id::TEXT,
				p_grupo_id::TEXT,
				p_persona_id::TEXT
			) tbl
			WHERE (
				tbl.persona_id IN (
					SELECT per.id
					FROM sbsep.persona per
					INNER JOIN sbsep.grupopersona gp ON gp.persona_id = per.id
					INNER JOIN  sbsep.grupo gr ON gr. ID = gp.grupo_id
					WHERE gp.estado <> 0
					AND gr.estado <> 0
					AND per.estado <> 0
					AND (
						gr.id::TEXT IN (SELECT UNNEST (string_to_array(p_grupo_id, ','))) OR TRIM(p_grupo_id) = ''
					)
					AND (
						per.id::TEXT NOT IN ( SELECT UNNEST ( string_to_array(p_excluir_personas_id, ',') ) ) OR TRIM(p_excluir_personas_id) = ''
					)
				)
			)
		) tbl
		WHERE age.id = tbl.agenda_id;
		RETURN TRUE;
	END IF;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER
COST 100;
--
DROP FUNCTION sbstar.up_agenda_traer_base_con_tiempos_calculados(
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_horario_id bigint,
	p_intervalo_id text,
	p_grupo_id text,
	p_persona_id text
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_traer_base_con_tiempos_calculados (
	IN p_fecha_inicio timestamp WITH TIME ZONE,
	IN p_fecha_fin timestamp WITH TIME ZONE,
	IN p_horario_id bigint,
	IN p_intervalo_id text,
	IN p_grupo_id text,
	IN p_persona_id text
)
RETURNS TABLE 
(
	agenda_id bigint,
	persona_id bigint,
	es_global smallint,
	horario_id bigint,
	horario_nombre varchar,
	intervalo_id bigint,
	intervalo_nombre varchar,
	tipo_intervalo_id bigint,
	es_intervalo_descanso boolean,
	horariodia_id bigint,
	horariodia_nombre varchar,
	horariodia_hora_inicio time WITH TIME ZONE,
	horariodia_inicio_diahora timestamp WITH TIME ZONE,
	horariodia_inicio_dia date,
	inicio_planificado timestamp WITH TIME ZONE,
	fin_planificado timestamp WITH TIME ZONE,
	inicio_real timestamp WITH TIME ZONE,
	fin_real timestamp WITH TIME ZONE,
	tiene_marcacion_inicio boolean,
	tiene_marcacion_fin boolean,
	tiene_marcaciones boolean,
	total_marcaciones smallint,
	inicio_sol_inconsistencia integer,
	fin_sol_inconsistencia integer,
	compensar_tardanzas smallint,
	inicio_planificado_compensado timestamp WITH TIME ZONE,
	fin_planificado_compensado timestamp WITH TIME ZONE,
	inicio_compensacion timestamp WITH TIME ZONE,
	fin_compensacion_teorica timestamp WITH TIME ZONE,
	fin_compensacion_real timestamp WITH TIME ZONE,
	tiene_tardanza boolean,
	fecha_tardanza timestamp WITH TIME ZONE,
	tiene_salida_anticipada boolean,
	fecha_salida_anticipada timestamp WITH TIME ZONE,
	tiene_excepciones_con_goze boolean,
	fechas_excepciones_con_goze timestamp WITH TIME ZONE[],
	tiene_excepciones_feriado boolean,
	fechas_excepciones_feriado timestamp WITH TIME ZONE[],
	tiene_excepciones_contratmplab boolean,
	fechas_excepciones_contratmplab timestamp WITH TIME ZONE[],
	tiene_excepciones_vacaciones boolean,
	teorico integer,
	usado_total integer,
	usado_en_teorico integer,
	extras_sinvalidar_total integer,
	extras_25 integer,
	extras_35 integer,
	autorizado integer,
	distribuir integer,
	reintegrar integer,
	no_autorizado integer,
	pago_personalizado integer,
	excepciones_vacaciones integer,
	excepciones_vacaciones_prog_noindemnizadas integer,
	excepciones_vacaciones_prog_indemnizadas integer,
	excepciones_subsidio_maternidad integer,
	excepciones_subsidio_incapacidad integer,
	excepciones_licencia_goze_lactancia integer,
	excepciones_licencia_goze_no_sistema integer,
	excepciones_con_goze integer,
	excepciones_sin_goze integer,
	excepciones_feriado integer,
	excepciones_contratmplab integer,
	tardanza integer,
	salida_anticipada integer,
	horas_nocturnas_total integer,
	horas_extras_total integer,
	horas_extras_25_en_laborable integer,
	horas_extras_35_en_laborable integer,
	horas_extras_total_en_laborable integer,
	horas_extras_25_en_descanso integer,
	horas_extras_35_en_descanso integer,
	horas_extras_total_en_descanso integer,
	horas_extras_25_en_feriado integer,
	horas_extras_35_en_feriado integer,
	horas_extras_total_en_feriado integer,
	horas_extras_25_en_feriado_laborable integer,
	horas_extras_35_en_feriado_laborable integer,
	horas_extras_total_en_feriado_laborable integer,
	horas_extras_25_en_feriado_descanso integer,
	horas_extras_35_en_feriado_descanso integer,
	horas_extras_total_en_feriado_descanso integer,
	tardanza_en_laborable integer,
	tardanza_en_descanso integer,
	salida_anticipada_en_laborable integer,
	salida_anticipada_en_descanso integer,
	laborado_teorico integer,
	laborado_real integer,
	laborado_en_teorico integer,
	laborado integer,
	laborado_total integer,
	descanso_teorico integer,
	descanso_teorico_con_marcaciones integer,
	laborado_en_descanso integer,
	laborado_real_en_descanso integer,
	inasistencia_en_laborable integer,
	feriado_teorico integer,
	excepciones_feriado_en_descanso integer,
	excepciones_feriado_en_laborable integer,
	laborado_feriado_en_descanso integer,
	laborado_feriado_en_laborable integer,
	laborado_feriado integer,
	feriado_teorico_en_descanso_con_marcaciones integer,
	feriado_teorico_en_laborable_con_marcaciones integer,
	feriado_teorico_con_marcaciones integer,
	inconsistencias integer,
	inconsistencias_en_laborable integer,
	inconsistencias_en_descanso integer,
	primerodemayo_remunerable smallint,
	tiempo_excepciones_intervalo integer
)
AS
$$
DECLARE
	TIPOINTERVALO_DESCANSO CONSTANT	bigint := 276;
BEGIN
	IF COALESCE(p_grupo_id,'') <> '' THEN
		SELECT public.agr_fila_unir(gp.persona_id::TEXT) INTO p_persona_id
		FROM sbsep.grupopersona gp 
		INNER JOIN sbsep.persona prs ON prs.id = gp.persona_id
		WHERE  (
			gp.persona_id IN (SELECT UNNEST(string_to_array(p_persona_id , ','))::BIGINT) OR
			COALESCE(p_persona_id,'') = ''
		)
		AND (
			gp.grupo_id IN (SELECT UNNEST(string_to_array(p_grupo_id , ','))::BIGINT) OR
			COALESCE(p_grupo_id,'') = ''
		)
		AND gp.estado = 1
		AND prs.estado = 1;
	END IF;
	--
	RETURN QUERY
	SELECT DISTINCT
		age.id,
		age.persona_id,
		age.es_global,
		hor.id AS horario_id,
		hor.nombre,
		ivo.id,
		ivo.nombre,
		ivo.tipo_intervalo_id,
		ivo.tipo_intervalo_id = TIPOINTERVALO_DESCANSO  AS es_intervalo_descanso,
		age.horariodia_id,
		null::varchar,
		null::time WITH TIME ZONE,
		null::timestamp WITH TIME ZONE,
		age.horariodia_inicio_dia,
		age.inicio_planificado,
		age.fin_planificado,
		age.inicio_real,
		age.fin_real,
		null::boolean,
		null::boolean,

		CASE
			WHEN age.tiene_marcaciones::smallint = 1 THEN
				TRUE
			ELSE FALSE
		END,
		age.total_marcaciones::smallint,
		age.inicio_sol_inconsistencia::INTEGER,
		age.fin_sol_inconsistencia::INTEGER,
		null::smallint,
		null::timestamp WITH TIME ZONE,
		null::timestamp WITH TIME ZONE,
		null::timestamp WITH TIME ZONE,
		null::timestamp WITH TIME ZONE,
		null::timestamp WITH TIME ZONE,
		null::boolean,
		null::timestamp WITH TIME ZONE,
		null::boolean,
		null::timestamp WITH TIME ZONE,
		null::boolean,
		null::timestamp WITH TIME ZONE[],
		null::boolean,
		null::timestamp WITH TIME ZONE[],
		null::boolean,
		null::timestamp WITH TIME ZONE[],
		null::boolean,
		COALESCE(age.teorico::INTEGER,0)::INTEGER,
		COALESCE(age.usado_total::INTEGER,0)::INTEGER,
		COALESCE(age.usado_en_teorico::INTEGER,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(age.autorizado::INTEGER,0)::INTEGER,
		COALESCE(age.distribuir::INTEGER,0)::INTEGER,
		COALESCE(age.reintegrar::INTEGER,0)::INTEGER,
		COALESCE(age.no_autorizado::INTEGER,0)::INTEGER,
		COALESCE(age.pago_personalizado::INTEGER,0)::INTEGER,
		COALESCE(age.excepciones_vacaciones::INTEGER,0)::INTEGER,
		COALESCE(age.exc_vac_prog_noindemnizadas::INTEGER,0)::INTEGER,
		COALESCE(age.exc_vac_prog_indemnizadas::INTEGER,0)::INTEGER,
		COALESCE(age.excepciones_subsidio_maternidad::INTEGER,0)::INTEGER,
		COALESCE(age.excepciones_subsidio_incapacida::INTEGER,0)::INTEGER,
		COALESCE(age.exc_licencia_goze_lactancia::INTEGER,0)::INTEGER,
		COALESCE(age.exc_licencia_goze_no_sistema::INTEGER,0)::INTEGER,
		COALESCE(age.excepciones_con_goze::INTEGER,0)::INTEGER,
		COALESCE(age.excepciones_sin_goze::INTEGER,0)::INTEGER,
		COALESCE(age.excepciones_feriado::INTEGER,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(age.tardanza::INTEGER,0)::INTEGER,
		COALESCE(age.salida_anticipada::INTEGER,0)::INTEGER,
		COALESCE(age.horas_nocturnas_total::INTEGER,0)::INTEGER,
		COALESCE(age.horas_extras_total::INTEGER,0)::INTEGER,
		COALESCE(age.horas_extras_25_en_laborable::INTEGER,0)::INTEGER,
		COALESCE(age.horas_extras_35_en_laborable::INTEGER,0)::INTEGER,
		COALESCE(age.he_total_en_laborable::INTEGER,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(age.he_total_en_descanso::INTEGER,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(age.he_total_en_feriado::INTEGER,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(age.he_total_en_feriado_laborable::INTEGER,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(null::integer,0)::INTEGER,
		COALESCE(age.he_total_en_feriado_descanso::INTEGER,0)::INTEGER,
		COALESCE(age.tardanza_en_laborable::INTEGER,0)::INTEGER,
		COALESCE(age.tardanza_en_descanso::INTEGER,0)::INTEGER,
		COALESCE(age.salida_anticipada_en_laborable::INTEGER,0)::INTEGER,
		COALESCE(age.salida_anticipada_en_descanso::INTEGER,0)::INTEGER,
		COALESCE(age.laborado_teorico::INTEGER,0)::INTEGER,
		COALESCE(age.laborado_real::INTEGER,0)::INTEGER,
		COALESCE(age.laborado_en_teorico::INTEGER,0)::INTEGER,
		COALESCE(age.laborado::INTEGER,0)::INTEGER,
		COALESCE(age.laborado_total::INTEGER,0)::INTEGER,
		COALESCE(age.descanso_teorico::INTEGER,0)::INTEGER,
		COALESCE(age.desc_teo_con_marcaciones::INTEGER,0)::INTEGER,
		COALESCE(age.laborado_en_descanso::INTEGER,0)::INTEGER,
		COALESCE(age.laborado_real_en_descanso::INTEGER,0)::INTEGER,
		COALESCE(age.inasistencia_en_laborable::INTEGER,0)::INTEGER,
		COALESCE(age.feriado_teorico::INTEGER,0)::INTEGER,
		COALESCE(age.exc_fer_en_descanso::INTEGER,0)::INTEGER,
		COALESCE(age.exc_fer_en_laborable::INTEGER,0)::INTEGER,
		COALESCE(age.laborado_feriado_en_descanso::INTEGER,0)::INTEGER,
		COALESCE(age.laborado_feriado_en_laborable::INTEGER,0)::INTEGER,
		COALESCE(age.laborado_feriado::INTEGER,0)::INTEGER,
		COALESCE(age.fer_teo_en_desc_con_marcaciones::INTEGER,0)::INTEGER,
		COALESCE(age.fer_teo_en_lab_con_marcaciones::INTEGER,0)::INTEGER,
		COALESCE(age.fer_teo_con_marcaciones::INTEGER,0)::INTEGER,
		COALESCE(age.inconsistencias::INTEGER,0)::INTEGER,
		COALESCE(age.inconsistencias_en_laborable::INTEGER,0)::INTEGER,
		COALESCE(age.inconsistencias_en_descanso::INTEGER,0)::INTEGER,
		COALESCE(age.primerodemayo_remunerable::SMALLINT,0)::SMALLINT,
		COALESCE(exc.tiempo_excepciones_intervalo::INTEGER,0)::INTEGER
	FROM sbstar.agenda age
	LEFT JOIN (
		SELECT
			age.id AS agenda_id,
			SUM(
				CASE 
					WHEN
					LEAST(age.fin_planificado,age.fin_real,ex.fecha_hora_fin) -
					GREATEST (age.inicio_planificado,age.inicio_real,ex.fecha_hora_inicio) > '00:00:00'::INTERVAL AND
					age.inicio_real NOTNULL AND age.fin_real NOTNULL THEN
						EXTRACT(epoch from
							LEAST(age.fin_planificado,ex.fecha_hora_fin) -
							GREATEST (age.inicio_planificado,ex.fecha_hora_inicio)
						)::integer
					ELSE
						0::INTEGER
				END
			) AS tiempo_excepciones_intervalo
		FROM sbstar.agenda age
		INNER JOIN sbstar.configuracionexcepcion cex ON cex.agenda_id = age.id 
		INNER JOIN sbstar.excepcion ex ON ex.agenda_id = age.id AND cex.estado = 1
		INNER JOIN sbstar.tipoexcepcion tex ON tex.id = cex.tipoexcepcion_id
		LEFT JOIN sbstar.intervalo ivo ON ivo.id = age.intervalo_id AND ivo.estado = 1
		AND (
			ivo.id IN (SELECT UNNEST(string_to_array(p_intervalo_id , ','))::BIGINT) OR
			COALESCE(p_intervalo_id, '') = '' 
		)
		LEFT JOIN sbstar.horario hor ON hor.id = ivo.horario_id AND hor.estado = 1
		AND (hor.id = p_horario_id OR p_horario_id ISNULL)
		WHERE (age.inicio_planificado >= p_fecha_inicio::TIMESTAMP WITH TIME ZONE  or p_fecha_inicio ISNULL)
		AND (age.fin_planificado <= p_fecha_fin::TIMESTAMP WITH TIME ZONE or p_fecha_fin ISNULL)
		AND (age.persona_id IN (SELECT UNNEST(string_to_array(p_persona_id , ','))::BIGINT) OR COALESCE(p_persona_id,'') = '' )
		AND age.estado = 1
		GROUP BY age.id
	) AS exc on exc.agenda_id = age.id
	LEFT JOIN sbstar.intervalo ivo ON ivo.id = age.intervalo_id AND ivo.estado = 1
	AND (
		ivo.id IN (
			SELECT UNNEST(string_to_array(p_intervalo_id , ','))::BIGINT) OR
		COALESCE(p_intervalo_id, '') = ''
	)
	LEFT JOIN sbstar.horario hor ON hor.id = ivo.horario_id AND hor.estado = 1
	AND (hor.id = p_horario_id OR p_horario_id ISNULL)
	WHERE (age.inicio_planificado >= p_fecha_inicio::TIMESTAMP WITH TIME ZONE  or p_fecha_inicio ISNULL)
	AND (age.inicio_planificado <= p_fecha_fin::TIMESTAMP WITH TIME ZONE or p_fecha_fin ISNULL)
	AND (age.persona_id IN (SELECT UNNEST(string_to_array(p_persona_id , ','))::BIGINT) OR COALESCE(p_persona_id,'') = '' )
	AND age.estado = 1;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;