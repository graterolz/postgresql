DROP FUNCTION sbstar.up_agenda_reprogramacion_simula(
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_intervalo_id bigint,
	p_persona_id bigint,
	p_grupo_id bigint,
	p_hora_ini varchar,
	p_hora_fin varchar,
	p_tipo_fin varchar,
	p_fecha_ini_nuevo timestamp WITH TIME ZONE
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_reprogramacion_simula (
	IN p_fecha_inicio timestamp WITH TIME ZONE,
	IN p_fecha_fin timestamp WITH TIME ZONE,
	IN p_intervalo_id bigint,
	IN p_persona_id bigint,
	IN p_grupo_id bigint,
	IN p_hora_ini varchar,
	IN p_hora_fin varchar,
	IN p_tipo_fin varchar,
	IN p_fecha_ini_nuevo timestamp WITH TIME ZONE
)
RETURNS TABLE 
(
	agenda_id bigint,
	inicio_planificado timestamp WITH TIME ZONE,
	fin_planificado timestamp WITH TIME ZONE,
	intervalo_id bigint,
	persona_id bigint,
	estado smallint,
	es_global smallint,
	agendahistorica_codigo text
)
AS
$$
BEGIN
	RETURN QUERY
	SELECT DISTINCT
		ageVirtual.agenda_id,
		CASE
			WHEN p_fecha_ini_nuevo IS NULL THEN (ageVirtual.inicio_planificado::timestamp::date || ' ' || p_hora_ini)::TIMESTAMP WITH time zone
			ELSE (p_fecha_ini_nuevo::timestamp::date || ' ' || p_hora_ini)::TIMESTAMP WITH time zone
		END as inicio_plan,
		CASE
			WHEN p_fecha_ini_nuevo IS NULL THEN
			(CASE
				WHEN p_tipo_fin = 'mismo_dia' THEN ageVirtual.inicio_planificado::timestamp::date || ' ' || p_hora_fin
				ELSE (ageVirtual.inicio_planificado::timestamp::date + interval'1 day')::date || ' ' || p_hora_fin
			END)::TIMESTAMP WITH time zone
			ELSE (
				(p_fecha_ini_nuevo::timestamp::date || ' ' || p_hora_ini)::TIMESTAMP WITH time zone +
				(SELECT
					public.dateDiff(
						'minute',
						ageVirtual.inicio_planificado::TIMESTAMP WITH time zone,
						ageVirtual.fin_planificado::TIMESTAMP WITH time zone
					) || ' minute')::INTERVAL)::TIMESTAMP WITH TIME ZONE 
			END as fin_plan,
		ageVirtual.intervalo_id,ageVirtual.persona_id,ageVirtual.estado,
		ageVirtual.es_global,ageVirtual.agendahistorica_codigo
	FROM sbstar.up_agenda_traer_x_intervalopadre_y_fechas(
		p_fecha_inicio, p_fecha_fin, p_intervalo_id, p_persona_id, p_grupo_id
	) ageVirtual
	ORDER BY inicio_plan, ageVirtual.agenda_id;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;
--
DROP FUNCTION sbstar.up_agenda_reprogramacion_registra(
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_intervalo_id bigint,
	p_persona_id bigint,
	p_grupo_id bigint,
	p_usuariocreador_id bigint,
	p_hora_ini varchar,
	p_hora_fin varchar,
	p_tipo_fin varchar,
	p_fecha_ini_nuevo timestamp WITH TIME ZONE
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_reprogramacion_registra (
	IN p_fecha_inicio timestamp WITH TIME ZONE,
	IN p_fecha_fin timestamp WITH TIME ZONE,
	IN p_intervalo_id bigint,
	IN p_persona_id bigint,
	IN p_grupo_id bigint,
	IN p_usuariocreador_id bigint,
	IN p_hora_ini varchar,
	IN p_hora_fin varchar,
	IN p_tipo_fin varchar,
	IN p_fecha_ini_nuevo timestamp WITH TIME ZONE
)
RETURNS TABLE 
(
	agenda_id bigint,
	inicio_planificado timestamp WITH TIME ZONE,
	fin_planificado timestamp WITH TIME ZONE,
	persona_id bigint,
	es_global smallint,
	agendahistorica_id bigint,
	agendahistorica_codigo text
)
AS
$$
BEGIN
	RETURN QUERY
	INSERT INTO sbstar.agenda(
		inicio_planificado,fin_planificado,intervalo_id,persona_id,estado,es_global,
		agendahistorica_id,usuariocreador_id,agendahistorica_codigo
	)
	SELECT
		ageVirtual.inicio_planificado,ageVirtual.fin_planificado,ageVirtual.intervalo_id,ageVirtual.persona_id,
		ageVirtual.estado,ageVirtual.es_global,ageVirtual.agenda_id,p_usuariocreador_id,ageVirtual.agendahistorica_codigo
	FROM sbstar.up_agenda_reprogramacion_simula(
		p_fecha_inicio,p_fecha_fin,p_intervalo_id,p_persona_id,p_grupo_id,
		p_hora_ini,p_hora_fin,p_tipo_fin,p_fecha_ini_nuevo
	) ageVirtual
	RETURNING 
		sbstar.agenda.id,
		sbstar.agenda.inicio_planificado,
		sbstar.agenda.fin_planificado,
		sbstar.agenda.persona_id,
		sbstar.agenda.es_global,
		sbstar.agenda.agendahistorica_id,
		sbstar.agenda.agendahistorica_codigo;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;
--
DROP FUNCTION sbstar.up_agenda_reprogramacion_simula(
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_intervalo_id bigint,
	p_persona_id bigint,
	p_grupo_id bigint,
	p_hora_ini varchar,
	p_hora_fin varchar,
	p_tipo_fin varchar,
	p_fecha_ini_nuevo timestamp WITH TIME ZONE
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_reprogramacion_simula (
	IN p_fecha_inicio timestamp WITH TIME ZONE,
	IN p_fecha_fin timestamp WITH TIME ZONE,
	IN p_intervalo_id bigint,
	IN p_persona_id bigint,
	IN p_grupo_id bigint,
	IN p_hora_ini varchar,
	IN p_hora_fin varchar,
	IN p_tipo_fin varchar,
	IN p_fecha_ini_nuevo timestamp WITH TIME ZONE
)
RETURNS TABLE 
(
	agenda_id bigint,
	inicio_planificado timestamp WITH TIME ZONE,
	fin_planificado timestamp WITH TIME ZONE,
	intervalo_id bigint,
	persona_id bigint,
	estado smallint,
	es_global smallint,
	agendahistorica_codigo text
)
AS
$$
BEGIN
	RETURN QUERY
	SELECT DISTINCT
		ageVirtual.agenda_id,
		CASE
			WHEN p_fecha_ini_nuevo IS NULL THEN
				(ageVirtual.inicio_planificado::timestamp::date || ' ' || p_hora_ini)::TIMESTAMP WITH time zone
			ELSE
				(p_fecha_ini_nuevo::timestamp::date || ' ' || p_hora_ini)::TIMESTAMP WITH time zone
		END as inicio_plan,
		CASE 
			WHEN p_fecha_ini_nuevo IS NULL THEN 
				(CASE
					WHEN p_tipo_fin = 'mismo_dia' THEN
						ageVirtual.inicio_planificado::timestamp::date || ' ' || p_hora_fin
					ELSE (ageVirtual.inicio_planificado::timestamp::date + interval'1 day')::date || ' ' || p_hora_fin
				END)::TIMESTAMP WITH time zone
			ELSE 
				(
					(p_fecha_ini_nuevo::timestamp::date || ' ' || p_hora_ini)::TIMESTAMP WITH time zone +
					(SELECT public.dateDiff(
						'minute',
						ageVirtual.inicio_planificado::TIMESTAMP WITH time zone,
						ageVirtual.fin_planificado::TIMESTAMP WITH time zone
					) || ' minute')::INTERVAL)::TIMESTAMP WITH TIME ZONE 
		END as fin_plan,
		ageVirtual.intervalo_id,ageVirtual.persona_id,ageVirtual.estado,
		ageVirtual.es_global, ageVirtual.agendahistorica_codigo
	FROM sbstar.up_agenda_traer_x_intervalopadre_y_fechas(
		p_fecha_inicio, p_fecha_fin, p_intervalo_id, p_persona_id, p_grupo_id
	) ageVirtual
	ORDER BY inicio_plan, ageVirtual.agenda_id;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;