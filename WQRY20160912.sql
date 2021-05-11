DROP FUNCTION sbstar.up_agenda_traer_base_con_tiempos_procesados_y_excp(
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_horario_id bigint,
	p_intervalo_id text,
	p_grupo_id text,
	p_persona_id text
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_traer_base_con_tiempos_procesados_y_excp
(
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
	periodoagenda_id bigint,
	agendahistorica_id bigint,
	es_global smallint,
	contratoplanilla_id bigint,
	horario_id bigint,
	horario_nombre varchar,
	tipo_acumulacion_he smallint,
	horariodia_id bigint,
	horariodia_nombre varchar,
	horariodia_hora_inicio time WITH TIME ZONE,
	horariodia_compara_hora smallint,
	horariodia_compara_planificado smallint,
	horariodia_inicio_diahora timestamp WITH TIME ZONE,
	horariodia_inicio_dia date,
	inicio_planificado timestamp WITH TIME ZONE,
	fin_planificado timestamp WITH TIME ZONE,
	inicio_real timestamp WITH TIME ZONE,
	fin_real timestamp WITH TIME ZONE,
	intervalo_id bigint,
	intervalo_nombre varchar,
	tipo_intervalo_id bigint,
	tipo_trabajo_id bigint,
	es_intervalo_descanso boolean,
	compensar_tardanzas smallint,
	inicio_limite_antes integer,
	inicio_limite_despues integer,
	inicio_tolerancia_despues integer,
	inicio_tiempo_extra_desde integer,
	inicio_tiempo_extra_hasta integer,
	inicio_acumulador_tiempo_extra_accion_id bigint,
	inicio_acumulador_tiempo_desechado_accion_id bigint,
	fin_limite_antes integer,
	fin_limite_despues integer,
	fin_tolerancia_antes integer,
	fin_tiempo_extra_desde integer,
	fin_tiempo_extra_hasta integer,
	fin_acumulador_tiempo_extra_accion_id bigint,
	fin_acumulador_tiempo_desechado_accion_id bigint,
	inicio_sol_inconsistencia smallint,
	fin_sol_inconsistencia smallint,
	asistencia double precision,
	tiempo_horas_nocturnas integer,
	he_por_pagar_25 double precision,
	he_por_pagar_35 double precision,
	he_por_pagar_personalizado double precision,
	autorizado double precision,
	no_autorizado double precision,
	por_distribuir double precision,
	por_compensar double precision,
	tiene_marcacion_inicio boolean,
	tiene_marcacion_fin boolean,
	tiene_marcaciones boolean,
	total_marcaciones smallint,
	fin_tolerancia_inicio timestamp WITH TIME ZONE,
	inicio_tolerancia_fin timestamp WITH TIME ZONE,
	tiempo_extra_inicio_sinvalidar integer,
	tiempo_extra_fin_sinvalidar integer,
	fecha_valida_desde_extra_inicio timestamp WITH TIME ZONE,
	fecha_valida_hasta_extra_inicio timestamp WITH TIME ZONE,
	fecha_valida_desde_extra_fin timestamp WITH TIME ZONE,
	fecha_valida_hasta_extra_fin timestamp WITH TIME ZONE,
	fecha_tardanza timestamp WITH TIME ZONE,
	fecha_salida_anticipada timestamp WITH TIME ZONE,
	tiempo_perdido_despues_inicio integer,
	tiempo_perdido_antes_fin integer,
	tiene_extra_inicio_valido boolean,
	tiene_extra_fin_valido boolean,
	tiene_tardanza boolean,
	tiempo_tardanza integer,
	tiempo_tardanza_con_tolerancia integer,
	tiene_salida_anticipada boolean,
	tiempo_salida_anticipada integer,
	tiempo_salida_anticipada_con_tolerancia integer,
	inicio_compensacion timestamp WITH TIME ZONE,
	fin_compensacion_teorica timestamp WITH TIME ZONE,
	fin_compensacion_real timestamp WITH TIME ZONE,
	inicio_planificado_compensado timestamp WITH TIME ZONE,
	fin_planificado_compensado timestamp WITH TIME ZONE,
	tiempo_compensado_teorico integer,
	tiempo_compensado_real integer,
	tiempo_real integer,
	tiempo_planificado integer,
	tiempo_real_en_planificado integer,
	tiempo_extra_inicio integer,
	tiempo_extra_fin integer,
	tiempo_desechado_inicio integer,
	tiempo_desechado_fin integer,
	tiempo_extras_total integer,
	tiempo_desechado_total integer,
	tiene_excepciones_con_goze boolean,
	tiempo_excepciones_con_goze integer,
	fechas_excepciones_con_goze timestamp WITH TIME ZONE[],
	tiene_excepciones_sin_goze boolean,
	tiempo_excepciones_sin_goze integer,
	fechas_excepciones_sin_goze timestamp WITH TIME ZONE[],
	tiene_excepciones_feriado boolean,
	tiempo_excepciones_feriado integer,
	fechas_excepciones_feriado timestamp WITH TIME ZONE[],
	tiene_excepciones_contratmplab boolean,
	tiempo_excepciones_contratmplab integer,
	fechas_excepciones_contratmplab timestamp WITH TIME ZONE[],
	fechas_excepciones_contratmplab_json text,
	tiene_excepciones_subsidio_maternidad boolean,
	tiempo_excepciones_subsidio_maternidad integer,
	fechas_excepciones_subsidio_maternidad timestamp WITH TIME ZONE[],
	tiene_excepciones_subsidio_incapacidad boolean,
	tiempo_excepciones_subsidio_incapacidad integer,
	fechas_excepciones_subsidio_incapacidad timestamp WITH TIME ZONE[],
	tiene_excepciones_licencia_goze_lactancia boolean,
	tiempo_excepciones_licencia_goze_lactancia integer,
	fechas_excepciones_licencia_goze_lactancia timestamp WITH TIME ZONE[],
	tiene_excepciones_vacaciones_prog_noindemnizadas boolean,
	tiempo_excepciones_vacaciones_prog_noindemnizadas integer,
	tiene_excepciones_vacaciones_prog_indemnizadas boolean,
	tiempo_excepciones_vacaciones_prog_indemnizadas integer,
	tiempo_excepciones_licencia_goze_no_sistema integer,
	tiempo_excepciones_goze_contratmplab integer,
	tiempo_excepciones_real_contratmplab integer,
	tiempo_excepciones_vacaciones_prog_noindemnizadas_contratmplab integer,
	tiempo_excepciones_vacaciones_prog_indemnizadas_contratmplab integer,
	tiempo_excepciones_sin_goze_contratmplab integer,
	tiempo_excepciones_feriado_contratmplab integer,
	tiempo_excepciones_subsidio_maternidad_contratmplab integer,
	tiempo_excepciones_subsidio_incapacidad_contratmplab integer,
	tiempo_excepciones_goze_lactancia_contratmplab integer,
	tiempo_excepciones_goze_no_sistema_contratmplab integer,
	excepciones_json text
)
AS
$$
DECLARE
	TIPOINTERVALO_DESCANSO CONSTANT	bigint := 276;
	TIPOEXCEPCION_CONGOZE CONSTANT bigint := 267;
	TIPOEXCEPCION_SINGOZE CONSTANT bigint := 268;
	TIPOEXCEPCION_FERIADO CONSTANT bigint := 344;
	TIPOEXCEPCION_CONTRATIEMPOLABORADO CONSTANT bigint := 333;
	TIPOEXCEPCION_VACACIONES CONSTANT bigint := 270;
	TIPOEXCEPCION_SUBSIDIO_MATERNIDAD CONSTANT bigint := -1;
	TIPOEXCEPCION_SUBSIDIO_INCAPACIDAD CONSTANT bigint := -2;
	TIPOEXCEPCION_LICENCIA_GOZE_LACTANCIA CONSTANT bigint := -3;
	TIPOEXCEPCION_LICENCIA_GOZE_NO_SISTEMA CONSTANT bigint := -4;
	CODIGO_TIPOEXCEPCION_SUBSIDIO_MATERNIDAD VARCHAR := 'SBSD.MAT';
	CODIGO_TIPOEXCEPCION_SUBSIDIO_INCAPACIDAD VARCHAR := 'SBSD.INCAP';
	CODIGO_TIPOEXCEPCION_LICENCIA_GOZE_LACTANCIA VARCHAR := 'LIC.GOCE.LACT';
	rd_agd RECORD;
	rd_exc RECORD;
	inicio_planificado TIMESTAMP WITH TIME ZONE;
	fin_planificado TIMESTAMP WITH TIME ZONE;
	tiempo_planificado INTEGER;	
	tiempo_real INTEGER;
	es_intervalo_descanso BOOLEAN;
	v_tiene_excepciones BOOLEAN;
	v_tiempo_excepciones INTEGER;
	v_fechas_excepciones TIMESTAMP WITH TIME ZONE[][];
	v_fechas_excepciones_json TEXT;
	v_tiempo_contratmplab_exc INTEGER;
	tiene_excepciones_con_goze BOOLEAN;
	tiempo_excepciones_con_goze INTEGER;
	fechas_excepciones_con_goze TIMESTAMP WITH TIME ZONE[][];	
	tiene_excepciones_sin_goze BOOLEAN;
	tiempo_excepciones_sin_goze INTEGER;
	fechas_excepciones_sin_goze TIMESTAMP WITH TIME ZONE[][];
	tiene_excepciones_feriado BOOLEAN;
	tiempo_excepciones_feriado INTEGER;
	fechas_excepciones_feriado TIMESTAMP WITH TIME ZONE[][];
	tiene_excepciones_contratmplab BOOLEAN;
	tiempo_excepciones_contratmplab INTEGER;
	fechas_excepciones_contratmplab TIMESTAMP WITH TIME ZONE[][];
	tiene_excepciones_vacaciones_prog_noindemnizadas BOOLEAN;
	tiempo_excepciones_vacaciones_prog_noindemnizadas INTEGER;
	tiene_excepciones_vacaciones_prog_indemnizadas BOOLEAN;
	tiempo_excepciones_vacaciones_prog_indemnizadas INTEGER;
	tiene_excepciones_subsidio_maternidad BOOLEAN;
	tiempo_excepciones_subsidio_maternidad INTEGER;
	fechas_excepciones_subsidio_maternidad TIMESTAMP WITH TIME ZONE[][];
	tiene_excepciones_subsidio_incapacidad BOOLEAN;
	tiempo_excepciones_subsidio_incapacidad INTEGER;
	fechas_excepciones_subsidio_incapacidad TIMESTAMP WITH TIME ZONE[][];
	tiene_excepciones_licencia_goze_lactancia BOOLEAN;
	tiempo_excepciones_licencia_goze_lactancia INTEGER;
	fechas_excepciones_licencia_goze_lactancia TIMESTAMP WITH TIME ZONE[][];
	tiempo_excepciones_licencia_goze_no_sistema INTEGER;
	fechas_excepciones_contratmplab_json TEXT;
	v_tiempo_contratmplab_real_calculado BOOLEAN;
	v_tiempo_contratmplab_real INTEGER;
	v_tiempo_contratmplab_en_horas_nocturnas_calculado BOOLEAN;
	v_tiempo_contratmplab_en_horas_nocturnas INTEGER;
	v_tiempo_contratmplab_goze INTEGER;
	v_tiempo_contratmplab_sin_goze INTEGER;
	v_tiempo_contratmplab_vacaciones_prog_noindemnizadas INTEGER;
	v_tiempo_contratmplab_vacaciones_prog_indemnizadas INTEGER;
	v_tiempo_contratmplab_feriado INTEGER;
	v_tiempo_contratmplab_subsidio_maternidad INTEGER;
	v_tiempo_contratmplab_subsidio_incapacidad INTEGER;
	v_tiempo_contratmplab_goze_lactancia INTEGER;
	v_tiempo_contratmplab_goze_no_sistema INTEGER;
	v_excepciones_json VARCHAR [];	
BEGIN
	FOR rd_agd IN SELECT agd.* FROM sbstar.up_agenda_traer_base_con_tiempos_procesados (
		p_fecha_inicio, p_fecha_fin, p_horario_id, 
		p_intervalo_id, p_grupo_id, p_persona_id
	) agd
	LOOP
		es_intervalo_descanso := rd_agd.tipo_intervalo_id = TIPOINTERVALO_DESCANSO;
		tiempo_planificado := rd_agd.tiempo_planificado;
		inicio_planificado := rd_agd.inicio_planificado;
		fin_planificado := rd_agd.fin_planificado;

		IF (rd_agd.compensar_tardanzas = 1 AND rd_agd.tiene_tardanza = true) THEN
			inicio_planificado := rd_agd.inicio_real;
			fin_planificado := rd_agd.fin_compensacion_teorica;
			tiempo_planificado := EXTRACT(epoch from fin_planificado - inicio_planificado)::integer;
		END IF;
		
		tiempo_real := EXTRACT(epoch from rd_agd.fin_real - rd_agd.inicio_real)::integer;

		tiene_excepciones_con_goze := false;
		tiempo_excepciones_con_goze := 0;
		fechas_excepciones_con_goze := null;
		
		tiene_excepciones_sin_goze := false;
		tiempo_excepciones_sin_goze := 0;
		fechas_excepciones_sin_goze := null;

		tiene_excepciones_feriado := false;
		tiempo_excepciones_feriado := 0;
		fechas_excepciones_feriado := null;

		tiene_excepciones_contratmplab := false;
		tiempo_excepciones_contratmplab := 0;
		fechas_excepciones_contratmplab := null;

		tiene_excepciones_vacaciones_prog_noindemnizadas := false;
		tiempo_excepciones_vacaciones_prog_noindemnizadas := 0;

		tiene_excepciones_vacaciones_prog_indemnizadas := false;
		tiempo_excepciones_vacaciones_prog_indemnizadas := 0;

		tiene_excepciones_subsidio_maternidad := false;
		tiempo_excepciones_subsidio_maternidad := 0;
		fechas_excepciones_subsidio_maternidad := null;

		tiene_excepciones_subsidio_incapacidad := false;
		tiempo_excepciones_subsidio_incapacidad := 0;
		fechas_excepciones_subsidio_incapacidad:= null;

		tiene_excepciones_licencia_goze_lactancia := false;
		tiempo_excepciones_licencia_goze_lactancia := 0;
		fechas_excepciones_licencia_goze_lactancia := null;

		tiempo_excepciones_licencia_goze_no_sistema := 0;

		fechas_excepciones_contratmplab_json := '';
		v_tiempo_contratmplab_real_calculado := FALSE;
		v_tiempo_contratmplab_real := 0;
		v_tiempo_contratmplab_en_horas_nocturnas_calculado := FALSE;
		v_tiempo_contratmplab_en_horas_nocturnas := 0;
		v_tiempo_contratmplab_goze := 0;
		v_tiempo_contratmplab_sin_goze  := 0;
		v_tiempo_contratmplab_vacaciones_prog_noindemnizadas := 0;
		v_tiempo_contratmplab_vacaciones_prog_indemnizadas := 0;
		v_tiempo_contratmplab_feriado := 0;
		v_tiempo_contratmplab_subsidio_maternidad := 0;
		v_tiempo_contratmplab_subsidio_incapacidad := 0;
		v_tiempo_contratmplab_goze_lactancia := 0;
		v_tiempo_contratmplab_goze_no_sistema := 0;
		v_excepciones_json := '';
		--RAISE NOTICE '=> %,%,%,%,%', inicio_planificado, fin_planificado, rd_agd.inicio_real, rd_agd.fin_real, rd_agd.agenda_id;

		FOR rd_exc IN WITH fn_excepciones_base as (
			SELECT 
				tpexc.tipo_id as tipoexcepcion_tipo_id, 
				tpexc.codigo as tipoexcepcion_codigo, 
				tpexc.sistema as tipoexcepcion_sistema,
				exc.fecha_hora_inicio, 
				exc.fecha_hora_fin,
				prog.indemnizacion_factor as vacacion_indemnizacion_factor,
				tpexc.nombre as tipoexcepcion_nombre,
				exc.id as excepcion_id,
				exc.configuracionexcepcion_id 
			FROM sbstar.tipoexcepcion tpexc
			INNER JOIN  sbstar.configuracionexcepcion cfg_exc ON cfg_exc.tipoexcepcion_id = tpexc."id"
			INNER JOIN  sbstar.excepcion exc ON exc.configuracionexcepcion_id = cfg_exc.id
			LEFT JOIN sbspla.vacacionesgozadas prog ON prog.configuracionexcepcion_id = cfg_exc.id
			WHERE tpexc.tipo_id IN (TIPOEXCEPCION_CONGOZE, TIPOEXCEPCION_FERIADO, TIPOEXCEPCION_CONTRATIEMPOLABORADO, TIPOEXCEPCION_VACACIONES, TIPOEXCEPCION_SINGOZE)
			AND cfg_exc.estado = 1 
			AND (
				(rd_agd.agenda_id = cfg_exc.agenda_id AND cfg_exc.agenda_id NOTNULL) OR
				(cfg_exc.es_global = 1 AND cfg_exc.agenda_id ISNULL) OR
				(cfg_exc.persona_id = rd_agd.persona_id AND cfg_exc.agenda_id ISNULL) OR
				(cfg_exc.grupo_id IN (
					SELECT grp_pe.grupo_id
					FROM sbsep.grupopersona grp_pe
					WHERE grp_pe.persona_id = rd_agd.persona_id
					AND grp_pe.estado = 1
				) AND cfg_exc.agenda_id ISNULL)
			)
		)
			SELECT
				dtExcepciones.indice_ordenacion, 
				dtExcepciones.tipoexcepcion_id, 
				dtExcepciones.vacacion_indemnizacion_factor,
				'[' || public.agr_concatenar(
					'{"inicio":"' || dtExcepciones.fecha_hora_inicio_ajustada || 
					'", "fin":"' || dtExcepciones.fecha_hora_fin_ajustada || 
					'"}'
				) || ']' AS fechas_ajustadas,
				'[' || public.agr_concatenar(
					'{"tipoExcepcionID":"' || dtExcepciones.tipoexcepcion_id ||
					'", "excepcionId":"' || dtExcepciones.excepcion_id ||
					'", "configuracionExcepcionId":"' || dtExcepciones.configuracionexcepcion_id ||
					'", "codigoTipoExcepcion":"' || dtExcepciones.tipoexcepcion_codigo ||
					'", "nombreTipoExcepcion":"' || dtExcepciones.tipoexcepcion_nombre ||
					'", "inicio":"' || dtExcepciones.fecha_hora_inicio_ajustada || 
					'", "fin":"' || dtExcepciones.fecha_hora_fin_ajustada ||
					'", "interseccion":"' || public.fn_intervalo_en_segundos(
						(LEAST(fin_planificado,dtExcepciones.fecha_hora_fin_ajustada) - 
							GREATEST(inicio_planificado,dtExcepciones.fecha_hora_inicio_ajustada))) ||
					'"}') || ']' AS excepciones_json
			FROM (
				SELECT
					CASE
						WHEN fn_base.tipoexcepcion_tipo_id IN (TIPOEXCEPCION_CONTRATIEMPOLABORADO)
							THEN 1
						ELSE 2
					END as indice_ordenacion,
					fn_base.tipoexcepcion_tipo_id as tipoexcepcion_id, 
					GREATEST(fn_base.fecha_hora_inicio, inicio_planificado) AS fecha_hora_inicio_ajustada,
					CASE
						WHEN fn_base.tipoexcepcion_tipo_id IN (TIPOEXCEPCION_VACACIONES)
							THEN GREATEST(fn_base.fecha_hora_fin, fin_planificado)
						ELSE LEAST(fn_base.fecha_hora_fin, fin_planificado)
					END as fecha_hora_fin_ajustada,
					COALESCE(fn_base.vacacion_indemnizacion_factor, 0) AS vacacion_indemnizacion_factor,
					fn_base.tipoexcepcion_nombre,
					fn_base.excepcion_id,
					fn_base.configuracionexcepcion_id,
					fn_base.tipoexcepcion_codigo
				FROM fn_excepciones_base fn_base
				WHERE (
					fn_base.tipoexcepcion_tipo_id IN (TIPOEXCEPCION_CONGOZE, TIPOEXCEPCION_CONTRATIEMPOLABORADO) AND
					(
						(fn_base.fecha_hora_inicio <= inicio_planificado AND fn_base.fecha_hora_fin >= fin_planificado) OR
						(fn_base.fecha_hora_inicio <= inicio_planificado AND fn_base.fecha_hora_fin >= inicio_planificado) OR
						(fn_base.fecha_hora_inicio <= fin_planificado AND fn_base.fecha_hora_fin >= fin_planificado) OR
						(fn_base.fecha_hora_inicio >= inicio_planificado AND fn_base.fecha_hora_fin <= fin_planificado)
					)
				)
				OR (
					fn_base.tipoexcepcion_tipo_id IN (TIPOEXCEPCION_FERIADO, TIPOEXCEPCION_VACACIONES) AND 
					(
						(fn_base.fecha_hora_inicio <= inicio_planificado AND fn_base.fecha_hora_fin >= fin_planificado) OR
						(fn_base.fecha_hora_inicio <= inicio_planificado AND fn_base.fecha_hora_fin >= inicio_planificado)
					)
				)
				UNION
				SELECT
					2 as indice_ordenacion,
					CASE 
						WHEN fn_base.tipoexcepcion_codigo = CODIGO_TIPOEXCEPCION_SUBSIDIO_MATERNIDAD
							THEN TIPOEXCEPCION_SUBSIDIO_MATERNIDAD 
						WHEN fn_base.tipoexcepcion_codigo = CODIGO_TIPOEXCEPCION_SUBSIDIO_INCAPACIDAD
							THEN TIPOEXCEPCION_SUBSIDIO_INCAPACIDAD 
						WHEN fn_base.tipoexcepcion_codigo = CODIGO_TIPOEXCEPCION_LICENCIA_GOZE_LACTANCIA
							THEN TIPOEXCEPCION_LICENCIA_GOZE_LACTANCIA
					END as tipoexcepcion_id,
					fn_base.fecha_hora_inicio, fn_base.fecha_hora_fin,
					COALESCE(fn_base.vacacion_indemnizacion_factor, 0),
					fn_base.tipoexcepcion_nombre,
					fn_base.excepcion_id,
					fn_base.configuracionexcepcion_id,
					fn_base.tipoexcepcion_codigo 
				FROM fn_excepciones_base fn_base
				WHERE fn_base.tipoexcepcion_tipo_id = TIPOEXCEPCION_CONGOZE 
				AND fn_base.tipoexcepcion_sistema = 1
				AND (
					(fn_base.fecha_hora_inicio <= inicio_planificado AND fn_base.fecha_hora_fin >= fin_planificado) OR
					(fn_base.fecha_hora_inicio <= inicio_planificado AND fn_base.fecha_hora_fin >= inicio_planificado) OR
					(fn_base.fecha_hora_inicio <= fin_planificado AND fn_base.fecha_hora_fin >= fin_planificado) OR
					(fn_base.fecha_hora_inicio >= inicio_planificado AND fn_base.fecha_hora_fin <= fin_planificado)
				)
				UNION
				SELECT 
					2 as indice_ordenacion,
					TIPOEXCEPCION_LICENCIA_GOZE_NO_SISTEMA as tipoexcepcion_id, 
					fn_base.fecha_hora_inicio, fn_base.fecha_hora_fin,
					COALESCE(fn_base.vacacion_indemnizacion_factor, 0),
					fn_base.tipoexcepcion_nombre,
					fn_base.excepcion_id,
					fn_base.configuracionexcepcion_id,
					fn_base.tipoexcepcion_codigo 
				FROM fn_excepciones_base fn_base
				WHERE fn_base.tipoexcepcion_tipo_id = TIPOEXCEPCION_CONGOZE 
				AND fn_base.tipoexcepcion_sistema = 0
				AND (
					(fn_base.fecha_hora_inicio <= inicio_planificado AND fn_base.fecha_hora_fin >= fin_planificado) OR
					(fn_base.fecha_hora_inicio <= inicio_planificado AND fn_base.fecha_hora_fin >= inicio_planificado) OR
					(fn_base.fecha_hora_inicio <= fin_planificado AND fn_base.fecha_hora_fin >= fin_planificado) OR
					(fn_base.fecha_hora_inicio >= inicio_planificado AND fn_base.fecha_hora_fin <= fin_planificado)
				)
				UNION
				SELECT
					3 as indice_ordenacion,
					TIPOEXCEPCION_SINGOZE as tipoexcepcion_id, 
					fn_base.fecha_hora_inicio, fn_base.fecha_hora_fin,
					COALESCE(fn_base.vacacion_indemnizacion_factor, 0),
					fn_base.tipoexcepcion_nombre,
					fn_base.excepcion_id,
					fn_base.configuracionexcepcion_id,
					fn_base.tipoexcepcion_codigo 
				FROM fn_excepciones_base fn_base
				WHERE fn_base.tipoexcepcion_tipo_id = TIPOEXCEPCION_SINGOZE
				AND (
					(fn_base.fecha_hora_inicio <= inicio_planificado AND fn_base.fecha_hora_fin >= fin_planificado) OR
					(fn_base.fecha_hora_inicio <= inicio_planificado AND fn_base.fecha_hora_fin >= inicio_planificado) OR
					(fn_base.fecha_hora_inicio <= fin_planificado AND fn_base.fecha_hora_fin >= fin_planificado) OR
					(fn_base.fecha_hora_inicio >= inicio_planificado AND fn_base.fecha_hora_fin <= fin_planificado)
				)
				ORDER BY indice_ordenacion, tipoexcepcion_id, fecha_hora_inicio_ajustada, fecha_hora_fin_ajustada
			) dtExcepciones
			GROUP BY dtExcepciones.indice_ordenacion, dtExcepciones.tipoexcepcion_id, dtExcepciones.vacacion_indemnizacion_factor

		LOOP
			SELECT
				tbl.tiene_excepciones,
				tbl.tiempo_excepciones,
				tbl.fechas_excepciones,
				fechas_excepciones_json,
				tiempo_contratmplab_exc
			INTO
				v_tiene_excepciones,
				v_tiempo_excepciones,
				v_fechas_excepciones,
				v_fechas_excepciones_json,
				v_tiempo_contratmplab_exc
			FROM sbstar.up_agenda_traer_base_con_tiempos_procesados_y_excp_base (
				rd_exc.fechas_ajustadas,
				inicio_planificado,
				fin_planificado,
				tiempo_planificado,
				fechas_excepciones_contratmplab_json
			) tbl;

			excepciones_json = rd_exc.excepciones_json;
			--RAISE NOTICE '=> %,%, %, %', rd_exc.tipoexcepcion_id, rd_exc.fechas_ajustadas, v_tiempo_excepciones, v_fechas_excepciones_json;
			--RAISE NOTICE '=> %,%, %, %, %', v_tiene_excepciones, v_tiempo_excepciones, v_fechas_excepciones, rd_exc.tipoexcepcion_id, TIPOEXCEPCION_CONGOZE;
			IF rd_exc.tipoexcepcion_id = TIPOEXCEPCION_CONTRATIEMPOLABORADO THEN
				tiene_excepciones_contratmplab := v_tiene_excepciones;
				tiempo_excepciones_contratmplab := v_tiempo_excepciones;
				fechas_excepciones_contratmplab := v_fechas_excepciones;
				fechas_excepciones_contratmplab_json := rd_exc.fechas_ajustadas;
			ELSEIF rd_exc.tipoexcepcion_id = TIPOEXCEPCION_CONGOZE THEN
				tiene_excepciones_con_goze := v_tiene_excepciones;
				tiempo_excepciones_con_goze := v_tiempo_excepciones;
				fechas_excepciones_con_goze := v_fechas_excepciones;
				v_tiempo_contratmplab_goze := v_tiempo_contratmplab_exc;
			ELSEIF rd_exc.tipoexcepcion_id = TIPOEXCEPCION_SINGOZE THEN
				tiene_excepciones_sin_goze := v_tiene_excepciones;
				tiempo_excepciones_sin_goze := v_tiempo_excepciones;
				fechas_excepciones_sin_goze := v_fechas_excepciones;
				v_tiempo_contratmplab_sin_goze := v_tiempo_contratmplab_exc;
			ELSEIF rd_exc.tipoexcepcion_id = TIPOEXCEPCION_FERIADO THEN
				tiene_excepciones_feriado := v_tiene_excepciones;
				tiempo_excepciones_feriado := v_tiempo_excepciones;
				fechas_excepciones_feriado := v_fechas_excepciones;
				v_tiempo_contratmplab_feriado := v_tiempo_contratmplab_exc;
			ELSEIF rd_exc.tipoexcepcion_id = TIPOEXCEPCION_VACACIONES THEN
				IF rd_exc.vacacion_indemnizacion_factor > 0 THEN
					tiene_excepciones_vacaciones_prog_indemnizadas := v_tiene_excepciones;
					tiempo_excepciones_vacaciones_prog_indemnizadas := v_tiempo_excepciones * rd_exc.vacacion_indemnizacion_factor;
					v_tiempo_contratmplab_vacaciones_prog_indemnizadas := v_tiempo_contratmplab_exc;
				ELSE
					tiene_excepciones_vacaciones_prog_noindemnizadas := v_tiene_excepciones;
					tiempo_excepciones_vacaciones_prog_noindemnizadas := v_tiempo_excepciones;
					v_tiempo_contratmplab_vacaciones_prog_noindemnizadas := v_tiempo_contratmplab_exc;
				END IF;
			ELSEIF rd_exc.tipoexcepcion_id = TIPOEXCEPCION_SUBSIDIO_MATERNIDAD THEN
				tiene_excepciones_subsidio_maternidad := v_tiene_excepciones;
				tiempo_excepciones_subsidio_maternidad := v_tiempo_excepciones;
				fechas_excepciones_subsidio_maternidad := v_fechas_excepciones;
				v_tiempo_contratmplab_subsidio_maternidad := v_tiempo_contratmplab_exc;
			ELSEIF rd_exc.tipoexcepcion_id = TIPOEXCEPCION_SUBSIDIO_INCAPACIDAD THEN
				tiene_excepciones_subsidio_incapacidad := v_tiene_excepciones;
				tiempo_excepciones_subsidio_incapacidad := v_tiempo_excepciones;
				fechas_excepciones_subsidio_incapacidad := v_fechas_excepciones;
				v_tiempo_contratmplab_subsidio_incapacidad := v_tiempo_contratmplab_exc;
			ELSEIF rd_exc.tipoexcepcion_id = TIPOEXCEPCION_LICENCIA_GOZE_LACTANCIA THEN
				tiene_excepciones_licencia_goze_lactancia:= v_tiene_excepciones;
				tiempo_excepciones_licencia_goze_lactancia := v_tiempo_excepciones;
				fechas_excepciones_licencia_goze_lactancia := v_fechas_excepciones;
				v_tiempo_contratmplab_goze_lactancia := v_tiempo_contratmplab_exc;
			ELSEIF rd_exc.tipoexcepcion_id = TIPOEXCEPCION_LICENCIA_GOZE_NO_SISTEMA THEN
				tiempo_excepciones_licencia_goze_no_sistema := v_tiempo_excepciones;
				v_tiempo_contratmplab_goze_no_sistema := v_tiempo_contratmplab_exc;
			END IF;
		END LOOP;
		--
		IF fechas_excepciones_contratmplab_json <> '' AND
			rd_agd.inicio_real NOTNULL AND
			rd_agd.fin_real NOTNULL AND
			v_tiempo_contratmplab_real_calculado = FALSE THEN
			SELECT
				SUM(EXTRACT(epoch from 
					LEAST((jsonTiempoLab->>'fin')::TIMESTAMP WITH TIME ZONE, rd_agd.fin_real) -
					GREATEST((jsonTiempoLab->>'inicio')::TIMESTAMP WITH TIME ZONE, rd_agd.inicio_real)	
				)::INTEGER) as tiempo_contratmplab_real
				INTO v_tiempo_contratmplab_real
			FROM json_array_elements(fechas_excepciones_contratmplab_json::json) jsonTiempoLab 
			WHERE (
				(jsonTiempoLab->>'inicio')::TIMESTAMP WITH TIME ZONE,
				(jsonTiempoLab->>'fin')::TIMESTAMP WITH TIME ZONE
			) OVERLAPS (rd_agd.inicio_real, rd_agd.fin_real);
			v_tiempo_contratmplab_real_calculado := TRUE;
		END IF;
		--
		IF fechas_excepciones_contratmplab_json <> '' AND 
			rd_agd.inicio_hora_nocturna NOTNULL AND
			rd_agd.fin_hora_nocturna NOTNULL AND
			v_tiempo_contratmplab_en_horas_nocturnas_calculado = FALSE THEN
			SELECT
				SUM(EXTRACT(epoch from 
					LEAST((jsonTiempoLab->>'fin')::TIMESTAMP WITH TIME ZONE, rd_agd.fin_real) -
					GREATEST((jsonTiempoLab->>'inicio')::TIMESTAMP WITH TIME ZONE, rd_agd.inicio_real)	
				)::INTEGER) as tiempo_contratmplab_en_horas_nocturnas
				INTO v_tiempo_contratmplab_en_horas_nocturnas
			FROM json_array_elements(fechas_excepciones_contratmplab_json::json) jsonTiempoLab 
			WHERE (
				(jsonTiempoLab->>'inicio')::TIMESTAMP WITH TIME ZONE,
				(jsonTiempoLab->>'fin')::TIMESTAMP WITH TIME ZONE
			) OVERLAPS (rd_agd.inicio_hora_nocturna, rd_agd.fin_hora_nocturna);

			v_tiempo_contratmplab_en_horas_nocturnas_calculado := TRUE;
		END IF;
		RETURN query
			SELECT
				rd_agd.agenda_id, rd_agd.persona_id, rd_agd.periodoagenda_id, rd_agd.agendahistorica_id,
				rd_agd.es_global, rd_agd.contratoplanilla_id, rd_agd.horario_id, rd_agd.horario_nombre, 
				rd_agd.tipo_acumulacion_he, rd_agd.horariodia_id, rd_agd.horariodia_nombre, rd_agd.horariodia_hora_inicio,
				rd_agd.horariodia_compara_hora, rd_agd.horariodia_compara_planificado, rd_agd.horariodia_inicio_diahora,
				rd_agd.horariodia_inicio_dia, rd_agd.inicio_planificado, rd_agd.fin_planificado, rd_agd.inicio_real,
				rd_agd.fin_real, rd_agd.intervalo_id, rd_agd.intervalo_nombre, rd_agd.tipo_intervalo_id, rd_agd.tipo_trabajo_id,
				es_intervalo_descanso, rd_agd.compensar_tardanzas, rd_agd.inicio_limite_antes, rd_agd.inicio_limite_despues,
				rd_agd.inicio_tolerancia_despues, rd_agd.inicio_tiempo_extra_desde, rd_agd.inicio_tiempo_extra_hasta,
				rd_agd.inicio_acumulador_tiempo_extra_accion_id, rd_agd.inicio_acumulador_tiempo_desechado_accion_id,
				rd_agd.fin_limite_antes, rd_agd.fin_limite_despues, rd_agd.fin_tolerancia_antes, rd_agd.fin_tiempo_extra_desde,
				rd_agd.fin_tiempo_extra_hasta, rd_agd.fin_acumulador_tiempo_extra_accion_id,
				rd_agd.fin_acumulador_tiempo_desechado_accion_id,rd_agd.inicio_sol_inconsistencia, rd_agd.fin_sol_inconsistencia,
				rd_agd.asistencia, rd_agd.tiempo_horas_nocturnas - COALESCE(v_tiempo_contratmplab_en_horas_nocturnas, 0),
				rd_agd.he_por_pagar_25, rd_agd.he_por_pagar_35, rd_agd.he_por_pagar_personalizado, rd_agd.autorizado,
				rd_agd.no_autorizado, rd_agd.por_distribuir, rd_agd.por_compensar, rd_agd.tiene_marcacion_inicio,
				rd_agd.tiene_marcacion_fin, rd_agd.tiene_marcaciones, rd_agd.total_marcaciones, rd_agd.fin_tolerancia_inicio,
				rd_agd.inicio_tolerancia_fin, rd_agd.tiempo_extra_inicio_sinvalidar, rd_agd.tiempo_extra_fin_sinvalidar,
				rd_agd.fecha_valida_desde_extra_inicio, rd_agd.fecha_valida_hasta_extra_inicio, rd_agd.fecha_valida_desde_extra_fin,
				rd_agd.fecha_valida_hasta_extra_fin, rd_agd.fecha_tardanza,
				CASE WHEN es_intervalo_descanso = FALSE THEN rd_agd.fecha_salida_anticipada ELSE NULL END,
				rd_agd.tiempo_perdido_despues_inicio,rd_agd.tiempo_perdido_antes_fin, rd_agd.tiene_extra_inicio_valido,
				rd_agd.tiene_extra_fin_valido, rd_agd.tiene_tardanza, rd_agd.tiempo_tardanza, rd_agd.tiempo_tardanza_con_tolerancia,
				CASE WHEN es_intervalo_descanso = FALSE THEN rd_agd.tiene_salida_anticipada ELSE FALSE END,
				CASE WHEN es_intervalo_descanso = FALSE THEN rd_agd.tiempo_salida_anticipada ELSE NULL END,
				CASE WHEN es_intervalo_descanso = FALSE THEN rd_agd.tiempo_salida_anticipada_con_tolerancia ELSE NULL END,
				rd_agd.inicio_compensacion,
				rd_agd.fin_compensacion_teorica,
				rd_agd.fin_compensacion_real,
				CASE WHEN rd_agd.compensar_tardanzas = 1 THEN 
					CASE WHEN rd_agd.tiene_tardanza = true THEN rd_agd.inicio_real
					ELSE rd_agd.inicio_planificado
					END
				ELSE rd_agd.inicio_planificado
				END as inicio_planificado_compensado,
				CASE WHEN rd_agd.compensar_tardanzas = 1 THEN 
					CASE WHEN rd_agd.tiene_tardanza = true THEN rd_agd.fin_compensacion_teorica
 					ELSE rd_agd.fin_planificado
					END
				ELSE rd_agd.fin_planificado
				END as fin_planificado_compensado,
				rd_agd.tiempo_compensado_teorico, rd_agd.tiempo_compensado_real, rd_agd.tiempo_real, rd_agd.tiempo_planificado,
				rd_agd.tiempo_real_en_planificado, rd_agd.tiempo_extra_inicio, rd_agd.tiempo_extra_fin, rd_agd.tiempo_desechado_inicio,
				rd_agd.tiempo_desechado_fin, rd_agd.tiempo_extra_inicio + rd_agd.tiempo_extra_fin,
				rd_agd.tiempo_desechado_inicio + rd_agd.tiempo_desechado_fin, tiene_excepciones_con_goze, tiempo_excepciones_con_goze,
				fechas_excepciones_con_goze, tiene_excepciones_sin_goze, tiempo_excepciones_sin_goze, fechas_excepciones_sin_goze,
				tiene_excepciones_feriado, tiempo_excepciones_feriado, fechas_excepciones_feriado, tiene_excepciones_contratmplab,
				tiempo_excepciones_contratmplab, fechas_excepciones_contratmplab, fechas_excepciones_contratmplab_json,
				tiene_excepciones_subsidio_maternidad, tiempo_excepciones_subsidio_maternidad, fechas_excepciones_subsidio_maternidad,
				tiene_excepciones_subsidio_incapacidad, tiempo_excepciones_subsidio_incapacidad, fechas_excepciones_subsidio_incapacidad,
				tiene_excepciones_licencia_goze_lactancia, tiempo_excepciones_licencia_goze_lactancia,
				fechas_excepciones_licencia_goze_lactancia, tiene_excepciones_vacaciones_prog_noindemnizadas,
				tiempo_excepciones_vacaciones_prog_noindemnizadas, tiene_excepciones_vacaciones_prog_indemnizadas,
				tiempo_excepciones_vacaciones_prog_indemnizadas, tiempo_excepciones_licencia_goze_no_sistema,
				COALESCE(v_tiempo_contratmplab_goze, 0), COALESCE(v_tiempo_contratmplab_real, 0),
				COALESCE(v_tiempo_contratmplab_vacaciones_prog_noindemnizadas, 0),
				COALESCE(v_tiempo_contratmplab_vacaciones_prog_indemnizadas, 0), COALESCE(v_tiempo_contratmplab_sin_goze, 0),
				COALESCE(v_tiempo_contratmplab_feriado, 0), COALESCE(v_tiempo_contratmplab_subsidio_maternidad, 0),
				COALESCE(v_tiempo_contratmplab_subsidio_incapacidad, 0), COALESCE(v_tiempo_contratmplab_goze_lactancia, 0),
				COALESCE(v_tiempo_contratmplab_goze_no_sistema, 0), v_excepciones_json;
	END LOOP;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;