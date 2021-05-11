DROP FUNCTION sbstar.up_busca_agendas_traslapes(
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_intervalo_id bigint,
	p_persona_id bigint,
	p_grupo_id bigint,
	p_hora_ini varchar,
	p_hora_fin varchar,
	p_tipo_fin varchar,
	p_horario_id bigint,
	p_fecha_ini_nuevo timestamp WITH TIME ZONE,
	p_agenda bigint
);
--
CREATE OR REPLACE FUNCTION sbstar.up_busca_agendas_traslapes (
	IN p_fecha_inicio timestamp WITH TIME ZONE,
	IN p_fecha_fin timestamp WITH TIME ZONE,
	IN p_intervalo_id bigint,
	IN p_persona_id bigint,
	IN p_grupo_id bigint,
	IN p_hora_ini varchar,
	IN p_hora_fin varchar,
	IN p_tipo_fin varchar,
	IN p_horario_id bigint,
	IN p_fecha_ini_nuevo timestamp WITH TIME ZONE,
	IN p_agenda bigint
)
RETURNS TABLE 
(
	agenda_id bigint,
	persona_id bigint,
	grupo_id bigint,
	horario_id bigint,
	intervalo_id bigint,
	fechaIni varchar,
	horaIni varchar,
	planIni timestamp WITH TIME ZONE,
	planFin timestamp WITH TIME ZONE,
	agendaCantidadHora varchar
)
AS
$$
DECLARE
	p_fechaIni varchar;
BEGIN
	SELECT TO_CHAR(inicio_planificado,'YYYY-MM-DD')::VARCHAR
	INTO p_fechaIni
	FROM sbstar.agenda age
	WHERE age.id = p_agenda;

	RETURN QUERY
	SELECT DISTINCT 
		age.id,
		age.persona_id,
		p_grupo_id grupo_id,
		tras.horario_id,
		age.intervalo_id,
		p_fechaIni fechaIni,
		to_char(age.inicio_planificado, 'HH24:MI')::VARCHAR horaIni,
		age.inicio_planificado planIni,
		age.fin_planificado planFin,
		COALESCE(
			public.fn_intervalo_en_horas(
				(age.fin_planificado-age.inicio_planificado)
			)::VARCHAR,'') agendaCantidadHora
	FROM sbstar.agenda age	
	INNER JOIN sbstar.up_agenda_reprogramacion_valida_traslapes(
		p_fecha_inicio, p_fecha_fin, p_intervalo_id, p_persona_id,p_grupo_id,
		p_hora_ini, p_hora_fin,p_tipo_fin,p_horario_id,p_fecha_ini_nuevo, p_agenda
	) tras ON age.id = tras.agenda_id;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;