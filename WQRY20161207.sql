DROP FUNCTION IF EXISTS sbstar.up_periodoagenda_traer_periodo_x_empresa (
	p_empresa_id bigint,
	p_tipoperiodo_id bigint
);
--
CREATE OR REPLACE FUNCTION sbstar.up_periodoagenda_traer_periodo_x_empresa (
	IN  p_empresa_id bigint,
	IN  p_tipoperiodo_id bigint
)
RETURNS TABLE 
(
	anio varchar,
	periodos text
)
AS
$$
DECLARE
	periodicidad varchar;
BEGIN 
	SELECT en.codigo INTO periodicidad
	FROM sbssys.enumeracion en
	INNER JOIN sbstar.tipoperiodo tp ON tp.id = p_tipoperiodo_id
	WHERE en.id = tp.tipo_id
	LIMIT 1;

	IF periodicidad = 'tpa_mes' THEN
		RETURN QUERY
		SELECT
			tbl.ejercicio as anio,
			'[' || PUBLIC.agr_fila_unir (
				'{ "fecha_inicio":"' || to_char(tbl.inicio, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT ||
				'", "fecha_fin":"' || to_char(tbl.fin, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT || 
				'", "nombre":"' || tbl.nombre :: TEXT || 
				'", "estado_cierre":"' || tbl.estado_cierre:: TEXT ||
				'", "tipoperiodo_id":"' || tbl.tipoperiodo_id:: TEXT ||
				'", "id": "' || tbl.id :: TEXT || '"}'
			) || ']' as periodos
		FROM (
			SELECT * FROM (
				SELECT
					pa.id,pa.ejercicio,pa.inicio,pa.nombre,pa.fin,
					COALESCE(pa.estado_cierre ,0) as estado_cierre,
					pa.tipoperiodo_id
				FROM sbstar.periodoagenda pa
				WHERE estado = 1 
				AND empresa_id = p_empresa_id
				AND tipoperiodo_id = p_tipoperiodo_id 
				AND COALESCE(estado_cierre ,0) = 0
				ORDER BY ejercicio, inicio ASC
				LIMIT 1
			) AS Abierto
			UNION ALL
			SELECT * FROM (
				SELECT
					pa.id,pa.ejercicio,pa.inicio,pa.nombre,pa.fin,
					COALESCE(pa.estado_cierre ,0) as estado_cierre,
					pa.tipoperiodo_id
				FROM sbstar.periodoagenda pa
				WHERE estado = 1 
				AND empresa_id = p_empresa_id 
				AND tipoperiodo_id = p_tipoperiodo_id
				AND COALESCE(estado_cierre ,0) = 1
				ORDER BY ejercicio DESC, inicio DESC
			) AS Cerrados
		) tbl
		GROUP BY tbl.ejercicio
		ORDER BY tbl.ejercicio DESC;
	ELSE
		RETURN QUERY
		SELECT
			tbl.ejercicio as anio,
			'[' || PUBLIC.agr_fila_unir (
				'{ "id":"' || COALESCE(tbl.id :: TEXT, '') ||
					'", "nombre":"' || tbl.nombre :: TEXT ||
					'", "fecha_fin":"' || to_char(tbl.fin, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT ||
					'", "fecha_inicio":"' ||to_char(tbl.inicio, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT ||
					'", "children": ' || COALESCE(tbl.children,'"-1"')|| '}'
			) || ']' as periodos
		FROM (
			SELECT * FROM (
				SELECT
					tblChild.id, pa.ejercicio,pa.inicio,pa.nombre,pa.fin,tblChild.children as children, 
					COALESCE(pa.estado_cierre ,0) as estado_cierre
				FROM sbstar.periodoagenda pa
				LEFT JOIN (
					SELECT
						tblpa.padre_id as id,
						'[' || PUBLIC.agr_fila_unir (
							'{ "fecha_inicio":"' || to_char(tblpa.inicio, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT ||
							'", "fecha_fin":"' || to_char(tblpa.fin, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT||
							'", "nombre":"' || tblpa.nombre :: TEXT ||
							'", "estado_cierre":"' || tblpa.estado_cierre:: TEXT ||
							'", "id": "' || tblpa.id :: TEXT || '"}'
						) || ']' as children
					FROM (
						SELECT * FROM (
							SELECT * FROM sbstar.up_periodoagenda_traer_childrens_x_empresa(p_empresa_id,p_tipoperiodo_id)
							WHERE COALESCE(estado_cierre ,0) = 0
							ORDER BY ejercicio, inicio ASC
							LIMIT 1
						) AS Abiertos
						UNION ALL
						SELECT * FROM (
							SELECT * FROM sbstar.up_periodoagenda_traer_childrens_x_empresa(p_empresa_id,p_tipoperiodo_id)
							WHERE COALESCE(estado_cierre ,0) = 1
							ORDER BY ejercicio DESC, inicio DESC										
						) AS Cerrados
					) tblpa
					GROUP BY tblpa.padre_id
				) tblChild ON pa.id = tblChild.id
				WHERE pa.estado = 1 
				AND pa.empresa_id = p_empresa_id 
				AND pa.tipoperiodo_id = p_tipoperiodo_id					
				AND pa.padre_id IS NULL
				AND COALESCE(estado_cierre ,0) = 0
				ORDER BY pa.ejercicio, pa.inicio ASC
				LIMIT 1
			) AS Abierto
			UNION ALL
			SELECT * FROM (
				SELECT
					tblChild.id, pa.ejercicio,pa.inicio,pa.nombre,pa.fin,tblChild.children as children,
					COALESCE(pa.estado_cierre ,0) as estado_cierre
				FROM sbstar.periodoagenda pa
				LEFT JOIN (
					SELECT
						tblpa.padre_id as id,
						'[' || PUBLIC.agr_fila_unir (
							'{ "fecha_inicio":"' || to_char(tblpa.inicio, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT ||
							'", "fecha_fin":"' || to_char(tblpa.fin, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT||
							'", "nombre":"' || tblpa.nombre :: TEXT ||
							'", "estado_cierre":"' || tblpa.estado_cierre:: TEXT ||
							'", "id": "' || tblpa.id :: TEXT || '"}'
						) || ']' as children
					FROM (
						SELECT * FROM (
							SELECT * FROM sbstar.up_periodoagenda_traer_childrens_x_empresa(p_empresa_id,p_tipoperiodo_id)
							WHERE COALESCE(estado_cierre ,0) = 0
							ORDER BY ejercicio, inicio ASC
							LIMIT 1
						) AS Abiertos
						UNION ALL
						SELECT * FROM (
							SELECT * FROM sbstar.up_periodoagenda_traer_childrens_x_empresa(p_empresa_id,p_tipoperiodo_id)
							WHERE COALESCE(estado_cierre ,0) = 1
							ORDER BY ejercicio DESC, inicio DESC
						) AS Cerrados
					) tblpa
					GROUP BY tblpa.padre_id
				) tblChild ON pa.id = tblChild.id
				WHERE pa.estado = 1 
				AND pa.empresa_id = p_empresa_id 
				AND pa.tipoperiodo_id = p_tipoperiodo_id					
				AND pa.padre_id IS NULL
				AND COALESCE(estado_cierre ,0) = 1
				ORDER BY pa.ejercicio DESC, pa.inicio DESC					
			) AS Cerrados
		) tbl
		GROUP BY tbl.ejercicio
		ORDER BY tbl.ejercicio DESC;
	END IF;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;
--
DROP FUNCTION sbspla.up_periodo_calcular (
	p_periodo_id bigint,
	p_usuariocreador_id bigint
);
--
CREATE OR REPLACE FUNCTION sbspla.up_periodo_calcular (
	IN p_periodo_id bigint,
	IN p_usuariocreador_id bigint
)
RETURNS BOOLEAN
AS
$$
DECLARE
	v_tipoperiodo_id bigint;
	v_ejercicio varchar;
	v_empresa_id bigint;
	v_padre_id bigint;
	v_periodo_abierto bigint;
	v_cantidad_periodo_cerrado numeric;
	v_cantidad_tiemporesumen numeric;
	v_periodo_id bigint;
	--
	v_cursor CURSOR FOR
		SELECT persona_id,por_compensar,exc_contra_por_compensar
		FROM sbspla.up_agenda_traer_por_compensar (p_periodo_id,NULL,0);
	cur_row RECORD;
	--
	v_persona_id bigint;
	v_por_compensar numeric;
	v_exc_contra_por_compensar numeric;
	v_saldo numeric;
	v_estado bigint;
BEGIN
	SELECT tipoperiodo_id, ejercicio, empresa_id, padre_id
	INTO v_tipoperiodo_id, v_ejercicio, v_empresa_id, v_padre_id
	FROM sbstar.periodoagenda
	WHERE id = p_periodo_id;
	--	
	IF(v_padre_id IS NULL) THEN
		SELECT id INTO v_periodo_abierto
		FROM sbstar.periodoagenda
		WHERE estado = 1
		AND tipoperiodo_id = v_tipoperiodo_id
		AND ejercicio = v_ejercicio
		AND empresa_id = v_empresa_id
		AND COALESCE(estado_cierre ,0) = 0
		AND PADRE_ID IS NULL
		ORDER BY inicio
		LIMIT 1;	
	ELSE
		SELECT id INTO v_periodo_abierto
		FROM sbstar.periodoagenda  
		WHERE estado = 1 	
		AND tipoperiodo_id = v_tipoperiodo_id 
		AND ejercicio = v_ejercicio
		AND empresa_id = v_empresa_id
		AND COALESCE(estado_cierre ,0) = 0
		AND PADRE_ID IS NOT NULL
		ORDER BY inicio
		LIMIT 1;
	END IF;
	--
	IF(v_periodo_abierto = p_periodo_id) THEN
		SELECT COUNT(*) INTO v_cantidad_tiemporesumen
		FROM sbstar.tiemporesumen
		WHERE periodoagenda_id = p_periodo_id;
		--						
		IF (v_cantidad_tiemporesumen > 0) THEN
			DELETE FROM sbstar.tiemporesumen
			WHERE periodoagenda_id = p_periodo_id;
		END IF;
		--
		SELECT COUNT(*) INTO v_cantidad_periodo_cerrado
		FROM sbstar.periodoagenda
		WHERE padre_id IS NULL
		AND tipoperiodo_id = v_tipoperiodo_id 
		AND ejercicio = v_ejercicio
		AND empresa_id = v_empresa_id
		AND COALESCE(estado_cierre ,0) = 1
		AND id <> p_periodo_id;
		--		
		IF (v_cantidad_periodo_cerrado > 0) THEN
			SELECT id INTO v_periodo_id
			FROM sbstar.periodoagenda
			WHERE padre_id IS NULL
			AND tipoperiodo_id = v_tipoperiodo_id
			AND ejercicio = v_ejercicio
			AND empresa_id = v_empresa_id
			AND COALESCE(estado_cierre ,0) = 1
			AND id <> p_periodo_id
			ORDER BY inicio DESC
			LIMIT 1;
		ELSE
			v_periodo_id = NULL;
		END IF;
		--
		OPEN v_cursor;
		LOOP
		FETCH v_cursor INTO cur_row;
		EXIT WHEN NOT FOUND;
			v_persona_id = cur_row.persona_id;
			v_por_compensar = cur_row.por_compensar;
			v_exc_contra_por_compensar = cur_row.exc_contra_por_compensar;
			--
			IF (v_periodo_id IS NOT NULL) THEN
				SELECT COALESCE(saldo, 0)::NUMERIC INTO v_saldo
				FROM sbstar.tiemporesumen
				WHERE periodoagenda_id = v_periodo_id
				AND persona_id = v_persona_id;
			ELSE
				v_saldo := 0;
			END IF;
			--
			v_saldo := (v_por_compensar - v_exc_contra_por_compensar) + v_saldo;
			v_estado := 1;
			--					
			INSERT INTO sbstar.tiemporesumen (
				periodoagenda_id,persona_id,usuariocreador_id,usuarioeditor_id,por_compensar,
				exc_contra_por_compensar,fecha_creacion,fecha_edicion,estado,saldo
			)
			VALUES (
				p_periodo_id,v_persona_id,p_usuariocreador_id,NULL,
				v_por_compensar,v_exc_contra_por_compensar,current_timestamp,
				current_timestamp,v_estado,v_saldo_anterior
			);
		END LOOP;
		CLOSE v_cursor;
		--
		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;
--
DROP FUNCTION sbsep.up_personas_traer_x_permisos(
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
	IN p_query text,
	IN p_modo text,
	IN p_grupo_id text,
	IN p_puesto_id text,
	IN p_usuario_id bigint,
	IN p_componenteinstancia_id bigint,
	IN p_control_id bigint,
	IN p_tipo_contrato text,
	IN p_grupos_recursivos smallint,
	IN p_dias_por_vencer bigint,
	IN p_culture text    
)
RETURNS TABLE 
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
	termino_contrato timestamp WITH TIME ZONE,
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

	FOREACH v_query IN ARRAY v_array_query
	LOOP
		IF(trim(v_query) <> '') THEN
			v_sql_query := v_sql_query || '(%' || lower(trim(v_query)) || '%)';
		END IF;
	END LOOP;
	--
	v_contratado:=false; v_sin_contrato:=false; v_renovado:=false; v_por_vencer:=false;
	--
	CASE p_tipo_contrato
		WHEN 'CTR' THEN v_contratado:=true;
		WHEN 'SCTR' THEN v_sin_contrato:=true;
		WHEN 'CTRR' THEN v_renovado:=true;
		WHEN 'CTRV' THEN v_por_vencer:=true;
	END CASE;
	--
	SELECT md.empresa_id INTO v_empresa_id
	FROM sbssys.componenteinstancia ci
	INNER JOIN sbsadm.modulo md ON md.id = ci.modulo_id 
	WHERE ci.id = p_componenteinstancia_id
	LIMIT 1;
	--
	SELECT public.agr_fila_unir(prs.id::TEXT) INTO v_personas_con_permisos_id
	FROM sbsep.up_personas_id_traer_x_permisos(
		p_modo,
		p_usuario_id,
		p_componenteinstancia_id,
		p_control_id,
		v_empresa_id
	) prs;
	--
	SELECT public.agr_fila_unir_unique(cp.id::TEXT) INTO v_contratopersonas_id
	FROM sbspla.contratoplanilla cp
	WHERE cp.planilla_id IS NOT NULL
	AND cp.persona_id IN (SELECT UNNEST(string_to_array(v_personas_con_permisos_id, ','))::BIGINT)
	AND now() BETWEEN cp.vigencia_inicio AND cp.vigencia_fin
	AND cp.estado = 1;
	--
	SELECT lower(valor) INTO v_lbl_dia
	FROM sbssys.up_idiomacontenido_traer_in_culture_x_id(p_culture,1219::BIGINT);
	--
	SELECT lower(valor) INTO v_lbl_dias
 	FROM sbssys.up_idiomacontenido_traer_in_culture_x_id(p_culture,2313::BIGINT);
 	--
 	SELECT lower(valor) INTO v_lbl_mes
 	FROM sbssys.up_idiomacontenido_traer_in_culture_x_id(p_culture,1944::BIGINT);
 	--
 	SELECT lower(valor) INTO v_lbl_meses
 	FROM sbssys.up_idiomacontenido_traer_in_culture_x_id(p_culture,3938::BIGINT);
 
	RAISE NOTICE
		'=> % | % | % | % | %',
		v_empresa_id,
		p_grupo_id,
		p_puesto_id,
		p_grupos_recursivos,
		v_contratopersonas_id;

	RETURN QUERY
	SELECT DISTINCT
		prs.id,
		prs.apellidos_nombres,
		prs.estado, usr.estado, 
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
		COALESCE(vac.vacaciones_formatted_tiempo_venc,'--') AS txt_tiempo_vencimiento_no_prog,
		COALESCE(
			CASE
				WHEN vac.vacaciones_no_programadas = 1 THEN
					'1 ' || v_lbl_dia
				ELSE vac.vacaciones_no_programadas::TEXT || ' ' || v_lbl_dias
			END,'--'
		) AS txt_vacaciones_no_programadas,
		COALESCE(
			CASE
				WHEN vac.vacaciones_pendientes_anteriores = 1 THEN
					'1 ' || v_lbl_dia
				ELSE vac.vacaciones_pendientes_anteriores::TEXT || ' días'
			END,'--'
		) AS txt_vacaciones_pendientes_anteriores,
		COALESCE(
			CASE
				WHEN vac.vacaciones_programadas = 1 THEN
					'1 ' || v_lbl_dia
				ELSE vac.vacaciones_programadas::TEXT || ' ' || v_lbl_dias
			END,'--'
		) AS txt_vacaciones_programadas,
		COALESCE(
			CASE
				WHEN vac.vacaciones_gozadas = 1 THEN
					'1 ' || v_lbl_dia
				ELSE vac.vacaciones_gozadas::TEXT || ' ' || v_lbl_dias
			END,'--'
		) AS txt_vacaciones_gozadas,
		COALESCE(
			CASE
				WHEN vac.vacaciones_ganadas_vigentes = 1 THEN
					'1 ' || v_lbl_dia
				ELSE vac.vacaciones_ganadas_vigentes::TEXT || ' ' || v_lbl_dias
			END,'--'
		) AS txt_vacaciones_ganadas_vigentes,
		COALESCE(
			CASE
				WHEN vac.vacaciones_gozadas_vigentes = 1 THEN
					'1 ' || v_lbl_dia
				ELSE vac.vacaciones_gozadas_vigentes::TEXT || ' ' || v_lbl_dias
			END,'--'
		) AS txt_vacaciones_gozadas_vigentes,
		COALESCE(vac.vacaciones_adiciones_tributarias::TEXT,'--') AS txt_vacaciones_adiciones_tributarias,
		prs.codigo,
		prs.email,
		hora.nombre,
		gpp.grupo_nombre,
		gpp.puesto_nombre,
		prs.telefono_sms,
		cp.vigencia_fin,
		pla.nombre::varchar,
		COALESCE(prs.id = cp.persona_id AND cp.termino_contrato_motivo_id IS NULL, false), 
		usr.id, 
		cp.id
	FROM sbsep.view_persona prs 
	INNER JOIN sbscrm.up_personas_grupos_puestos(
		v_empresa_id,
		p_grupo_id,
		p_puesto_id,
		p_grupos_recursivos,
		p_culture
	) gpp ON gpp.persona_id = prs.id
	LEFT JOIN sbscrm.vw_personas_ambito_fv_cta prsAmb ON prsAmb.persona_id = prs.id 
	LEFT JOIN (
		SELECT DISTINCT
			cp.persona_id,
			first_value(cp.id) OVER (PARTITION BY cp.persona_id ORDER BY cp.vigencia_inicio desc) AS contrato_id
		FROM sbspla.contratoplanilla cp
		WHERE cp.estado = 1 
		AND cp.vigencia_inicio < now()
		AND cp.persona_id IN (
			SELECT UNNEST(string_to_array(v_personas_con_permisos_id, ','))::BIGINT
		)
	) dtContrato ON dtContrato.persona_id = prs.id
	LEFT JOIN sbspla.contratoplanilla cp ON cp.id = dtContrato.contrato_id
	LEFT JOIN (
		SELECT DISTINCT
			persona_id,
			first_value(pper.planilla_id) OVER (PARTITION BY pper.persona_id ORDER BY pper.fecha_inicio desc) AS planilla_id
		FROM sbspla.planillapersona pper
		WHERE pper.estado = 1 
		AND  pper.fecha_inicio < now()
		AND pper.persona_id IN (
			SELECT UNNEST(string_to_array(v_personas_con_permisos_id, ','))::BIGINT
		)
	) dtPlanilla ON dtPlanilla.persona_id = prs.id
	LEFT JOIN sbspla.planilla pla ON pla.id = dtPlanilla.planilla_id
	LEFT JOIN sbsep.tipopersona tipoPrs ON tipoPrs.id = prs.tipopersona_id
	LEFT JOIN sbsadm.usuario usr ON usr.persona_id = prs.id AND usr.estado <> 0
	LEFT JOIN (
		SELECT DISTINCT
			ph.persona_id,
			first_value(ph.horario_id) OVER (PARTITION BY ph.persona_id ORDER BY ph.vigencia_inicio desc) AS horario_id
		FROM sbstar.personahorario ph
		WHERE ph.estado = 1 
		AND ph.vigencia_inicio < now()
		AND ph.persona_id IN (
			SELECT UNNEST(string_to_array(v_personas_con_permisos_id, ','))::BIGINT
		)
	) dtHorario ON dtHorario.persona_id = prs.id
	LEFT JOIN sbstar.horario hora ON hora.id = dtHorario.horario_id
	LEFT JOIN sbscrm.up_personas_contratos_estado(p_dias_por_vencer) pct ON pct.persona_id = prs.id
	LEFT JOIN sbstar.up_configuracionexcepcion_bolsatiempo_vacaciones_by_contrato(
		v_contratopersonas_id,
		now(),
		v_lbl_dia,
		v_lbl_dias,
		v_lbl_mes,
		v_lbl_meses
	) vac ON vac.persona_id = prs.id
	WHERE prs.estado <> 0
	AND prs.tipo_id = 218
	AND (
		lower(prs.nombre_completo) SIMILAR TO v_sql_query OR
		tipoPrs.nombre ILIKE '%' || p_query || '%' OR
		prsAmb.persona_ambito ILIKE '%' || p_query || '%' OR
		prs.dni ILIKE '%' || p_query || '%' OR
		prs.codigo ILIKE '%' || p_query || '%' OR
		prs.email ILIKE '%' || p_query || '%' OR
		prs.telefono_sms ILIKE '%' || p_query || '%' OR
		hora.nombre ILIKE '%' || p_query || '%' OR
		gpp.grupo_nombre ILIKE '%' || p_query || '%' OR
		gpp.puesto_nombre ILIKE '%' || p_query || '%' 
	)
	AND (
		(v_contratado=false AND v_sin_contrato=false AND v_renovado=false AND v_por_vencer=false) OR
		(v_contratado=true AND pct.contratado=v_contratado) OR
		(v_sin_contrato=true AND pct.sin_contrato=v_sin_contrato) OR
		(v_renovado=true AND pct.renovado=v_renovado) OR
		(v_por_vencer=true AND pct.por_vencer=v_por_vencer)
	)
	AND prs.id IN (SELECT UNNEST(string_to_array(v_personas_con_permisos_id, ','))::BIGINT);
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;