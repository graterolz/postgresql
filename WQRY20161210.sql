DROP FUNCTION sbstar.up_agenda_calcular_tiempos_to_save(
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_horario_id bigint,
	p_intervalo_id text,
	p_grupo_id text,
	p_persona_id text
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_calcular_tiempos_to_save (
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
	tiene_excepciones_sin_goze boolean,
	fechas_excepciones_sin_goze timestamp WITH TIME ZONE[],
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
	excepciones_json varchar
)
AS
$$
DECLARE
	TIEMPOEXTRA_A_PAGAR CONSTANT bigint := 281;
	TIEMPOEXTRA_AUTORIZADO CONSTANT bigint := 280;
	TIEMPOEXTRA_DISTRIBUIR CONSTANT bigint := 282;
	TIEMPOEXTRA_POR_COMPENSAR CONSTANT bigint := 329;
	TIEMPOEXTRA_NO_AUTORIZADO CONSTANT bigint := 283;
	TIEMPODESECHADO_AUTORIZADO CONSTANT bigint := 334;
	TIEMPODESECHADO_DISTRIBUIR CONSTANT bigint := 335;
	TIEMPODESECHADO_NO_AUTORIZADO CONSTANT bigint := 336;
BEGIN
	RETURN query
		SELECT
			agd_bas1.*,
			(CASE
				WHEN agd_bas1.inicio_real NOTNULL
				AND agd_bas1.fin_real NOTNULL
					THEN COALESCE(agd_bas1.extras_25 + agd_bas1.extras_35, 0)
				ELSE 0 
			END) as horas_extras_total,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false
					THEN agd_bas1.extras_25
				ELSE 0
			END) as horas_extras_25_en_laborable,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false
					THEN agd_bas1.extras_35
				ELSE 0
			END) as horas_extras_35_en_laborable,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false 
					THEN COALESCE(agd_bas1.extras_25 + agd_bas1.extras_35, 0)
				ELSE 0
			END) as horas_extras_total_en_laborable,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
					THEN agd_bas1.extras_25
				ELSE 0
			END) as horas_extras_25_en_descanso,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
					THEN agd_bas1.extras_35
				ELSE 0
			END) as horas_extras_35_en_descanso,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true 
					THEN COALESCE(agd_bas1.extras_25 + agd_bas1.extras_35, 0)
				ELSE 0
			END) as horas_extras_total_en_descanso,
			(CASE
				WHEN agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true 
					THEN agd_bas1.extras_25
				ELSE 0
			END) as horas_extras_25_en_feriado,
			(CASE
				WHEN agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true 
					THEN agd_bas1.extras_35
				ELSE 0
			END) as horas_extras_35_en_feriado,
			(CASE
				WHEN agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true
					THEN COALESCE(agd_bas1.extras_25 + agd_bas1.extras_35, 0)
				ELSE 0
			END) as horas_extras_total_en_feriado,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false
				AND agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true
					THEN agd_bas1.extras_25
				ELSE 0
			END) as horas_extras_25_en_feriado_laborable,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false
				AND agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true 
					THEN agd_bas1.extras_35
				ELSE 0
			END) as horas_extras_35_en_feriado_laborable,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false
				AND agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true
					THEN COALESCE(agd_bas1.extras_25 + agd_bas1.extras_35, 0)
				ELSE 0
			END) as horas_extras_total_en_feriado_laborable,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
				AND agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true 
					THEN agd_bas1.extras_25 ELSE 0
			END) as horas_extras_25_en_feriado_descanso,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
				AND agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true 
					THEN agd_bas1.extras_35 ELSE 0
			END) as horas_extras_35_en_feriado_descanso,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
				AND agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true
					THEN COALESCE(agd_bas1.extras_25 + agd_bas1.extras_35, 0)
				ELSE 0
			END) as horas_extras_total_en_feriado_descanso,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false
					THEN agd_bas1.tardanza
				ELSE 0
			END) as tardanza_en_laborable,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
					THEN agd_bas1.tardanza
				ELSE 0
			END) as tardanza_en_descanso,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false
					THEN agd_bas1.salida_anticipada
				ELSE 0
			END) as salida_anticipada_en_laborable,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
					THEN agd_bas1.salida_anticipada
				ELSE 0
			END) as salida_anticipada_en_descanso,
			(CASE
				WHEN (agd_bas1.tipo_intervalo_id = 625)
					THEN 0
				WHEN agd_bas1.es_intervalo_descanso = false
				AND agd_bas1.tiene_excepciones_feriado = false
					THEN agd_bas1.teorico
				ELSE 0
			END) as laborado_teorico,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false
				AND agd_bas1.tiene_excepciones_feriado = false
					THEN agd_bas1.usado_total
				ELSE 0
			END) as laborado_real,
			(CASE
				WHEN (agd_bas1.tipo_intervalo_id = 625)
					THEN 0
				WHEN agd_bas1.es_intervalo_descanso = false
				AND agd_bas1.tiene_excepciones_feriado = false
					THEN agd_bas1.usado_en_teorico
				ELSE 0
			END) as laborado_en_teorico,
			(CASE
				WHEN (agd_bas1.tipo_intervalo_id = 625)
					THEN 0
				WHEN agd_bas1.es_intervalo_descanso = false
				AND agd_bas1.tiene_excepciones_feriado = false
					THEN 
						(CASE
							WHEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze) <= agd_bas1.teorico
								THEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze)
							ELSE agd_bas1.teorico
						END)
				ELSE 0
			END) as laborado,
			(CASE
				WHEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze) <= agd_bas1.teorico
					THEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze)
				ELSE agd_bas1.teorico
			END) as laborado_total,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
					THEN agd_bas1.teorico
				ELSE 0
			END) as descanso_teorico,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
				AND agd_bas1.tiene_marcaciones = true
					THEN agd_bas1.teorico
				ELSE 0
			END) as descanso_teorico_con_marcaciones,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true 
					THEN 
						(CASE
							WHEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze) <= agd_bas1.teorico
								THEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze)
							ELSE agd_bas1.teorico
						END)
				ELSE 0
			END) as laborado_en_descanso,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
					THEN agd_bas1.usado_total
				ELSE 0
			END) as laborado_real_en_descanso,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false
				AND agd_bas1.tiene_marcaciones = false 
				AND agd_bas1.tiene_excepciones_con_goze = false
				AND agd_bas1.tiene_excepciones_feriado = false
				AND agd_bas1.tiene_excepciones_vacaciones = false
					THEN agd_bas1.teorico
				ELSE 0
			END) as inasistencia_en_laborable,
			(CASE
				WHEN agd_bas1.tiene_excepciones_feriado = true
					THEN agd_bas1.teorico
				ELSE 0
			END) as feriado_teorico,
			(CASE 
				WHEN agd_bas1.es_intervalo_descanso = true
					THEN agd_bas1.excepciones_feriado
				ELSE 0 
			END) as excepciones_feriado_en_descanso,
			(CASE 
				WHEN agd_bas1.es_intervalo_descanso = false
					THEN agd_bas1.excepciones_feriado
				ELSE 0 
			END) as excepciones_feriado_en_laborable,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
				AND agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true
					THEN 
						(CASE
							WHEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze) <= agd_bas1.teorico
								THEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze)
							ELSE agd_bas1.teorico
						END)
				ELSE 0
			END) as laborado_feriado_en_descanso,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false
				AND agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true
					THEN
						(CASE
							WHEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze) <= agd_bas1.teorico
								THEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze)
							ELSE agd_bas1.teorico
						END)
				ELSE 0
			END) as laborado_feriado_en_laborable,
			(CASE
				WHEN agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true
					THEN
						(CASE
							WHEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze) <= agd_bas1.teorico
								THEN (agd_bas1.usado_en_teorico + agd_bas1.excepciones_con_goze)
							ELSE agd_bas1.teorico
						END)
				ELSE 0
			END) as laborado_feriado,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true
				AND agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true
					THEN agd_bas1.teorico
				ELSE 0
			END) as feriado_teorico_en_descanso_con_marcaciones,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false
				AND agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true
					THEN agd_bas1.teorico
				ELSE 0
			END) as feriado_teorico_en_laborable_con_marcaciones,
			(CASE 
				WHEN agd_bas1.tiene_excepciones_feriado = true
				AND agd_bas1.tiene_marcaciones = true
					THEN agd_bas1.teorico
				ELSE 0
			END) as feriado_teorico_con_marcaciones,
			agd_bas1.inicio_sol_inconsistencia + agd_bas1.fin_sol_inconsistencia as inconsistencias,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = false 
					THEN agd_bas1.inicio_sol_inconsistencia + agd_bas1.fin_sol_inconsistencia 
				ELSE 0
			END) as inconsistencias_en_laborable,
			(CASE
				WHEN agd_bas1.es_intervalo_descanso = true 
					THEN agd_bas1.inicio_sol_inconsistencia + agd_bas1.fin_sol_inconsistencia 
				ELSE 0
			END) as inconsistencias_en_descanso,
			(CASE 
				WHEN to_char(agd_bas1.inicio_planificado, 'DDMM') = '0105'
					THEN
						(CASE
							WHEN (agd_bas1.tiene_excepciones_feriado = true AND agd_bas1.tiene_marcaciones = true)
							OR (agd_bas1.tiene_excepciones_feriado = true AND agd_bas1.es_intervalo_descanso = true)
								THEN 1
							ELSE 0
						END)
				ELSE 0
			END)::smallint as primerodemayo_remunerable,
			agd_bas1.excepciones_json
		FROM (
			SELECT 
				agd.agenda_id,
				agd.persona_id,
				agd.es_global,
				agd.horario_id,
				agd.horario_nombre,
				agd.intervalo_id,
				agd.intervalo_nombre,
				agd.tipo_intervalo_id,
				agd.es_intervalo_descanso,
				agd.horariodia_id,
				agd.horariodia_nombre,
				agd.horariodia_hora_inicio,
				agd.horariodia_inicio_diahora,
				agd.horariodia_inicio_dia,
				agd.inicio_planificado,
				agd.fin_planificado,
				agd.inicio_real,
				agd.fin_real,
				agd.tiene_marcacion_inicio,
				agd.tiene_marcacion_fin,
				agd.tiene_marcaciones,
				agd.total_marcaciones,
				(CASE
					WHEN COALESCE(agd.inicio_sol_inconsistencia, 0) > 0
						THEN 1
					ELSE 0
				END)::integer as inicio_sol_inconsistencia,
				(CASE
					WHEN COALESCE(agd.fin_sol_inconsistencia, 0) > 0
						THEN 1
					ELSE 0
				END)::integer as fin_sol_inconsistencia,
				agd.compensar_tardanzas,
				agd.inicio_planificado_compensado,
				agd.fin_planificado_compensado,
				agd.inicio_compensacion,
				agd.fin_compensacion_teorica,
				agd.fin_compensacion_real,
				agd.tiene_tardanza,
				agd.fecha_tardanza,
				agd.tiene_salida_anticipada,
				agd.fecha_salida_anticipada,
				agd.tiene_excepciones_con_goze,
				agd.fechas_excepciones_con_goze,
				agd.tiene_excepciones_sin_goze,
				agd.fechas_excepciones_sin_goze,
				agd.tiene_excepciones_feriado,
				agd.fechas_excepciones_feriado,
				agd.tiene_excepciones_contratmplab,
				agd.fechas_excepciones_contratmplab,
				(
					agd.tiene_excepciones_vacaciones_prog_noindemnizadas OR
					agd.tiene_excepciones_vacaciones_prog_indemnizadas
				) as tiene_excepciones_vacaciones,
				(CASE
					WHEN agd.compensar_tardanzas = 1
						THEN EXTRACT(epoch from agd.fin_planificado_compensado - agd.inicio_planificado_compensado)::INTEGER
					ELSE agd.tiempo_planificado
				END) - agd.tiempo_excepciones_contratmplab as teorico,
				(COALESCE(EXTRACT(epoch from agd.fin_real - agd.inicio_real), 0)::integer - agd.tiempo_excepciones_real_contratmplab) as usado_total,
				COALESCE(
					(CASE 
						WHEN NOT agd.asistencia IS NULL 
							THEN agd.asistencia
						WHEN agd.compensar_tardanzas = 1 AND agd.tiene_tardanza = true
							THEN EXTRACT(epoch from 
								(CASE
									WHEN agd.fin_real <= agd.fin_planificado_compensado
										THEN agd.fin_real
									WHEN NOT agd.fin_real IS NULL
										THEN agd.fin_planificado_compensado
								END) -
								agd.inicio_real)::integer
							ELSE agd.tiempo_real_en_planificado
					END),
				0)::integer - agd.tiempo_excepciones_real_contratmplab as usado_en_teorico,
				COALESCE(agd.tiempo_extras_total, 0)::integer as extras_total,
				COALESCE(
					(CASE
						WHEN NOT agd.he_por_pagar_25 IS NULL 
							THEN agd.he_por_pagar_25
						WHEN agd.tipo_acumulacion_he <> 1
							THEN 0
						WHEN agd.inicio_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_A_PAGAR
						AND agd.fin_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_A_PAGAR
							THEN
								(CASE
									WHEN agd.tiempo_extras_total <= 2*3600 
										THEN agd.tiempo_extras_total
									ELSE 2*3600
								END)
						WHEN agd.inicio_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_A_PAGAR
							THEN
								(CASE
									WHEN agd.tiempo_extra_inicio <= 2*3600 
										THEN agd.tiempo_extra_inicio
									ELSE 2*3600
								END)
						WHEN agd.fin_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_A_PAGAR
							THEN
								(CASE
									WHEN agd.tiempo_extra_fin <= 2*3600 
										THEN agd.tiempo_extra_fin
									ELSE 2*3600
								END)
						ELSE 0
					END),
				0)::integer as extras_25,
				COALESCE(
					(CASE
						WHEN NOT agd.he_por_pagar_35 IS NULL 
							THEN agd.he_por_pagar_35
						WHEN agd.tipo_acumulacion_he <> 1
							THEN 0
						WHEN agd.inicio_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_A_PAGAR
						AND agd.fin_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_A_PAGAR
							THEN
								(CASE
									WHEN agd.tiempo_extras_total > 2*3600 
										THEN agd.tiempo_extras_total - 2*3600
									ELSE 0
								END)
						WHEN agd.inicio_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_A_PAGAR
							THEN
								(CASE
									WHEN agd.tiempo_extra_inicio > 2*3600 
										THEN agd.tiempo_extra_inicio - 2*3600
									ELSE 0
								END)
						WHEN agd.fin_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_A_PAGAR
							THEN
								(CASE
									WHEN agd.tiempo_extra_fin > 2*3600 
										THEN agd.tiempo_extra_fin - 2*3600
									ELSE 0
								END)
						ELSE 0
					END),
				0)::integer as extras_35,
				COALESCE(
					(CASE
						WHEN NOT agd.autorizado IS NULL 
							THEN (CASE WHEN agd.inicio_real NOTNULL AND agd.fin_real NOTNULL THEN agd.autorizado ELSE 0 END)
						ELSE
							(
								(CASE
									WHEN agd.inicio_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_AUTORIZADO
										THEN agd.tiempo_extra_inicio
									ELSE 0
								END)
								+
								(CASE
									WHEN agd.fin_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_AUTORIZADO
										THEN agd.tiempo_extra_fin
									ELSE 0
								END)
								+
								(CASE
									WHEN agd.inicio_acumulador_tiempo_desechado_accion_id = TIEMPODESECHADO_AUTORIZADO
										THEN agd.tiempo_desechado_inicio
									ELSE 0
								END)
								+
								(CASE
									WHEN agd.fin_acumulador_tiempo_desechado_accion_id = TIEMPODESECHADO_AUTORIZADO
										THEN agd.tiempo_desechado_fin
									ELSE 0
								END)
							)
					END),
				0)::integer as autorizado,
				COALESCE(
					(CASE
						WHEN NOT agd.por_distribuir IS NULL 
							THEN
								(CASE
									WHEN agd.inicio_real NOTNULL
									AND agd.fin_real NOTNULL
										THEN agd.por_distribuir
									ELSE 0
								END)
						ELSE
							(
								(CASE
									WHEN agd.inicio_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_DISTRIBUIR
										THEN agd.tiempo_extra_inicio
									ELSE 0 
								END)
								+
								(CASE
									WHEN agd.fin_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_DISTRIBUIR
										THEN agd.tiempo_extra_fin
									ELSE 0
								END)
								+
								(CASE
									WHEN agd.inicio_acumulador_tiempo_desechado_accion_id = TIEMPODESECHADO_DISTRIBUIR
										THEN agd.tiempo_desechado_inicio
									ELSE 0
								END)
								+
								(CASE
									WHEN agd.fin_acumulador_tiempo_desechado_accion_id = TIEMPODESECHADO_DISTRIBUIR
										THEN agd.tiempo_desechado_fin
									ELSE 0
								END)
							)
					END),
				0)::integer as distribuir,
				COALESCE(
					(CASE
						WHEN NOT agd.por_compensar IS NULL 
							THEN (CASE WHEN agd.inicio_real NOTNULL AND agd.fin_real NOTNULL THEN agd.por_compensar ELSE 0 END)
						ELSE
							(CASE
								WHEN agd.inicio_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_POR_COMPENSAR
									THEN agd.tiempo_extra_inicio
								ELSE 0
							END)
							+
							(CASE
								WHEN agd.fin_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_POR_COMPENSAR
									THEN agd.tiempo_extra_fin
								ELSE 0
							END)
							+
							(CASE
								WHEN agd.inicio_acumulador_tiempo_desechado_accion_id = TIEMPOEXTRA_POR_COMPENSAR
									THEN agd.tiempo_desechado_inicio
								ELSE 0
							END)
							+
							(CASE
								WHEN agd.fin_acumulador_tiempo_desechado_accion_id = TIEMPOEXTRA_POR_COMPENSAR
									THEN agd.tiempo_desechado_fin
								ELSE 0
							END)
					END),
				0)::integer as reintegrar,
				COALESCE(
					(CASE
						WHEN NOT agd.no_autorizado IS NULL 
							THEN
								(CASE
									WHEN agd.inicio_real NOTNULL
									AND agd.fin_real NOTNULL
										THEN agd.no_autorizado
									ELSE 0
								END)
						ELSE
							(CASE
								WHEN agd.inicio_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_NO_AUTORIZADO
									THEN agd.tiempo_extra_inicio
								ELSE 0
							END)
							+
							(CASE
								WHEN agd.fin_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_NO_AUTORIZADO
									THEN agd.tiempo_extra_fin
								ELSE 0
							END)
							+
							(CASE
								WHEN agd.inicio_acumulador_tiempo_desechado_accion_id = TIEMPODESECHADO_NO_AUTORIZADO
									THEN agd.tiempo_desechado_inicio
								ELSE 0
							END)
							+
							(CASE
								WHEN agd.fin_acumulador_tiempo_desechado_accion_id = TIEMPODESECHADO_NO_AUTORIZADO
									THEN agd.tiempo_desechado_fin
								ELSE 0
							END)
					END),
				0)::integer as no_autorizado,
				COALESCE(
					(CASE
						WHEN NOT agd.he_por_pagar_personalizado IS NULL 
							THEN 
								(CASE
									WHEN agd.inicio_real NOTNULL
									AND agd.fin_real NOTNULL
										THEN agd.he_por_pagar_personalizado
									ELSE 0 
								END)
						ELSE
							(CASE
								WHEN agd.tipo_acumulacion_he = 2
								AND agd.inicio_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_A_PAGAR 
									THEN agd.tiempo_extra_inicio
								ELSE 0
							END)
							+
							(CASE
								WHEN agd.tipo_acumulacion_he = 2
								AND agd.fin_acumulador_tiempo_extra_accion_id = TIEMPOEXTRA_A_PAGAR 
									THEN agd.tiempo_extra_fin
								ELSE 0
							END)
					END),
				0)::integer as pago_personalizado,
				(
					agd.tiempo_excepciones_vacaciones_prog_noindemnizadas - 
					agd.tiempo_excepciones_vacaciones_prog_noindemnizadas_contratmplab +
					agd.tiempo_excepciones_vacaciones_prog_indemnizadas - 
					agd.tiempo_excepciones_vacaciones_prog_indemnizadas_contratmplab
				) as excepciones_vacaciones,				
				(
					agd.tiempo_excepciones_vacaciones_prog_noindemnizadas -
					agd.tiempo_excepciones_vacaciones_prog_noindemnizadas_contratmplab
				) as excepciones_vacaciones_prog_noindemnizadas,
				(
					agd.tiempo_excepciones_vacaciones_prog_indemnizadas -
					agd.tiempo_excepciones_vacaciones_prog_indemnizadas_contratmplab
				) as excepciones_vacaciones_prog_indemnizadas,
				(
					agd.tiempo_excepciones_subsidio_maternidad -
					agd.tiempo_excepciones_subsidio_maternidad_contratmplab
				) as excepciones_subsidio_maternidad,
				(
					agd.tiempo_excepciones_subsidio_incapacidad -
					agd.tiempo_excepciones_subsidio_incapacidad_contratmplab
				) as excepciones_subsidio_incapacidad,
				(
					agd.tiempo_excepciones_licencia_goze_lactancia -
					agd.tiempo_excepciones_goze_lactancia_contratmplab
				) as excepciones_licencia_goze_lactancia,
				(
					agd.tiempo_excepciones_licencia_goze_no_sistema - 
					agd.tiempo_excepciones_goze_no_sistema_contratmplab
				) as excepciones_licencia_goze_no_sistema,
				(
					agd.tiempo_excepciones_con_goze -
					agd.tiempo_excepciones_goze_contratmplab
				) as excepciones_con_goze,
				(
					agd.tiempo_excepciones_sin_goze - 
					agd.tiempo_excepciones_sin_goze_contratmplab
				) as excepciones_sin_goze,
				(
					agd.tiempo_excepciones_feriado -
					agd.tiempo_excepciones_feriado_contratmplab
				) as excepciones_feriado,
				agd.tiempo_excepciones_contratmplab as excepciones_contratmplab,
				(CASE
					WHEN (agd.tipo_intervalo_id = 625)
						THEN 0
					WHEN agd.compensar_tardanzas = 1
						THEN 0
					ELSE agd.tiempo_tardanza_con_tolerancia
				END) as tardanza,
				(CASE 
					WHEN (agd.tipo_intervalo_id = 625)
						THEN 0
					WHEN agd.compensar_tardanzas = 1
					AND agd.tiene_tardanza = true
						THEN
							(CASE
								WHEN EXTRACT(epoch from agd.fin_planificado_compensado - agd.fin_real) > COALESCE(agd.fin_tolerancia_antes,0)
									THEN EXTRACT(epoch from agd.fin_planificado_compensado - agd.fin_real)::integer
								ELSE 0
							END)
					ELSE agd.tiempo_salida_anticipada_con_tolerancia
				END) as salida_anticipada,
				agd.tiempo_horas_nocturnas,
				agd.excepciones_json
			FROM sbstar.up_agenda_traer_base_con_tiempos_procesados_y_excp (
				p_fecha_inicio, p_fecha_fin, p_horario_id, p_intervalo_id, p_grupo_id, p_persona_id
			) agd
		) as agd_bas1;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;