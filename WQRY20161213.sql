DROP FUNCTION sbstar.up_agenda_reprogramacion_valida_traslapes(
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_intervalo_id bigint,
	p_persona_id bigint,
	p_grupo_id bigint,
	p_hora_ini varchar,
	p_hora_fin varchar,
	p_tipo_fin varchar,
	p_horario_id bigint,
	p_fecha_ini_nuevo timestamp WITH TIME ZONE
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_reprogramacion_valida_traslapes (
	IN p_fecha_inicio timestamp WITH TIME ZONE,
	IN p_fecha_fin timestamp WITH TIME ZONE,
	IN p_intervalo_id bigint,
	IN p_persona_id bigint,
	IN p_grupo_id bigint,
	IN p_hora_ini varchar,
	IN p_hora_fin varchar,
	IN p_tipo_fin varchar,
	IN p_horario_id bigint,
	IN p_fecha_ini_nuevo timestamp WITH TIME ZONE
)
RETURNS TABLE 
(
	agenda_id bigint,
	inicio_planificado timestamp WITH TIME ZONE,
	fin_planificado timestamp WITH TIME ZONE,
	persona_id bigint,
	persona_nombre_completo varchar,
	intervalo_id bigint,
	intervalo_nombre varchar,
	fecha_cruce varchar,
	horario_id bigint,
	horario_nombre varchar
)
AS
$$
DECLARE
	v_fecha_inicio TIMESTAMP WITH TIME ZONE;
	v_fecha_fin TIMESTAMP WITH TIME ZONE;
BEGIN
	v_fecha_inicio := p_fecha_inicio;
	v_fecha_fin := p_fecha_fin;
	--
	IF p_fecha_ini_nuevo IS NOT NULL THEN 
		v_fecha_inicio := (p_fecha_ini_nuevo)::TIMESTAMP WITH TIME ZONE;
		v_fecha_fin := (
			(p_fecha_ini_nuevo::TIMESTAMP WITH TIME ZONE + ('1 day')::INTERVAL)::TIMESTAMP WITH TIME ZONE -
			('1 second')::INTERVAL)::TIMESTAMP WITH TIME ZONE;
	END IF;
	--
	v_fecha_inicio := v_fecha_inicio - ('1 day')::INTERVAL;
	v_fecha_fin := v_fecha_fin + ('1 day')::INTERVAL;

	RETURN QUERY
	SELECT
		age.id,
		age.inicio_planificado,
		age.fin_planificado,
		age.persona_id,
		vp.nombre_completo::VARCHAR,
		age.intervalo_id,
		intv.nombre,
		CASE 
			WHEN age.inicio_planificado BETWEEN ageVirtual.inicio_planificado AND ageVirtual.fin_planificado THEN
				to_char(age.inicio_planificado, 'DD/MM/YYYY')::VARCHAR
			WHEN age.fin_planificado BETWEEN ageVirtual.inicio_planificado AND ageVirtual.fin_planificado THEN
				to_char(age.fin_planificado, 'DD/MM/YYYY')::VARCHAR
			WHEN ageVirtual.inicio_planificado BETWEEN age.inicio_planificado AND age.fin_planificado THEN
				to_char(ageVirtual.inicio_planificado, 'DD/MM/YYYY')::VARCHAR
			WHEN ageVirtual.fin_planificado BETWEEN age.inicio_planificado AND age.fin_planificado THEN
				to_char(ageVirtual.fin_planificado, 'DD/MM/YYYY')::VARCHAR
		END::VARCHAR AS fecha_cruce,
		hora.id,
		hora.nombre
	FROM sbstar.agenda age
	INNER JOIN sbstar.up_agenda_reprogramacion_simula(
		p_fecha_inicio,
		p_fecha_fin,
		p_intervalo_id,
		p_persona_id,
		p_grupo_id,
		p_hora_ini,
		p_hora_fin,
		p_tipo_fin,
		p_fecha_ini_nuevo
	) ageVirtual ON COALESCE(ageVirtual.persona_id, 0) = COALESCE(age.persona_id, 0)
	LEFT JOIN sbsep.view_persona vp ON vp.id = age.persona_id
	LEFT JOIN sbstar.intervalo intv ON intv.id = age.intervalo_id
	LEFT JOIN sbstar.horario hora ON hora.id = intv.horario_id
	WHERE age.estado = 1 
	AND (age.id <> ageVirtual.agenda_id)
	AND (vp.estado ISNULL OR vp.estado <> 0)
	AND ((age.inicio_planificado, age.fin_planificado) OVERLAPS (v_fecha_inicio, v_fecha_fin))
	AND ((age.inicio_planificado, age.fin_planificado) OVERLAPS (ageVirtual.inicio_planificado, ageVirtual.fin_planificado))
	AND (age.persona_id IS NOT NULL OR (age.persona_id ISNULL AND hora.id = p_horario_id));
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;
--
DROP FUNCTION sbstar.up_agenda_reprogramacion_valida_traslapes_virtuales(
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_intervalo_id bigint,
	p_persona_id bigint,
	p_grupo_id bigint,
	p_hora_ini varchar,
	p_hora_fin varchar,
	p_tipo_fin varchar,
	p_horario_id bigint,
	p_fecha_ini_nuevo timestamp WITH TIME ZONE
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_reprogramacion_valida_traslapes_virtuales(
	IN p_fecha_inicio timestamp WITH TIME ZONE,
	IN p_fecha_fin timestamp WITH TIME ZONE,
	IN p_intervalo_id bigint,
	IN p_persona_id bigint,
	IN p_grupo_id bigint,
	IN p_hora_ini varchar,
	IN p_hora_fin varchar,
	IN p_tipo_fin varchar,
	IN p_horario_id bigint,
	IN p_fecha_ini_nuevo timestamp WITH TIME ZONE
)
RETURNS TABLE 
(
	persona_id bigint,
	persona_nombre_completo varchar,
	intervalo_id bigint,
	intervalo_nombre varchar,
	fecha_cruce varchar,
	horario_id bigint,
	horario_nombre varchar
)
AS
$$
BEGIN
	RETURN query
	SELECT DISTINCT
		t1.persona_id,
		vp.nombre_completo::VARCHAR,
		t1.intervalo_id,
		intv.nombre,
		CASE
			WHEN t1.inicio_planificado BETWEEN t2.inicio_planificado AND t2.fin_planificado THEN
				to_char(t1.inicio_planificado, 'DD/MM/YYYY')::VARCHAR
			WHEN t1.fin_planificado BETWEEN t2.inicio_planificado AND t2.fin_planificado THEN
				to_char(t1.fin_planificado, 'DD/MM/YYYY')::VARCHAR
			WHEN t2.inicio_planificado BETWEEN t1.inicio_planificado AND t1.fin_planificado THEN
				to_char(t2.inicio_planificado, 'DD/MM/YYYY')::VARCHAR
			WHEN t2.fin_planificado BETWEEN t1.inicio_planificado AND t1.fin_planificado THEN
				to_char(t2.fin_planificado, 'DD/MM/YYYY')::VARCHAR
		END::VARCHAR AS fecha_cruce,
		hora.id,
		hora.nombre
	FROM sbstar.up_agenda_reprogramacion_simula(
		p_fecha_inicio,
		p_fecha_fin,
		p_intervalo_id,
		p_persona_id,
		p_grupo_id,
		p_hora_ini,
		p_hora_fin,
		p_tipo_fin,p_fecha_ini_nuevo
	) AS t1
	INNER JOIN sbstar.up_agenda_reprogramacion_simula(
		p_fecha_inicio,
		p_fecha_fin,
		p_intervalo_id,
		p_persona_id,
		p_grupo_id,
		p_hora_ini,
		p_hora_fin,
		p_tipo_fin,
		p_fecha_ini_nuevo
	) AS t2 ON t2.agenda_id <> t1.agenda_id AND (COALESCE(t1.persona_id, 0) = COALESCE(t2.persona_id, 0))
	LEFT JOIN sbsep.view_persona vp ON vp.id = t1.persona_id
	LEFT JOIN sbstar.intervalo intv ON intv.id = t1.intervalo_id
	LEFT JOIN sbstar.horario hora ON hora.id = intv.horario_id
	WHERE ((t1.inicio_planificado, t1.fin_planificado) OVERLAPS(t2.inicio_planificado, t2.fin_planificado))
	AND (vp.estado ISNULL OR vp.estado <> 0)
	ORDER BY fecha_cruce, vp.nombre_completo::VARCHAR DESC;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;