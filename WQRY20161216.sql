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
	p_fecha_ini_nuevo timestamp WITH TIME ZONE,
	p_agenda bigint
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_reprogramacion_valida_traslapes_virtuales (
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
	RETURN QUERY
	SELECT DISTINCT
		t1.persona_id, vp.nombre_completo::VARCHAR, t1.intervalo_id, intv.nombre,
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
		hora.id, hora.nombre
	FROM sbstar.up_agenda_reprogramacion_simula(
		p_fecha_inicio, p_fecha_fin, p_intervalo_id, p_persona_id, p_grupo_id,
		p_hora_ini, p_hora_fin, p_tipo_fin,p_fecha_ini_nuevo
	) AS t1
	INNER JOIN sbstar.up_agenda_reprogramacion_simula(
		p_fecha_inicio, p_fecha_fin, p_intervalo_id, p_persona_id, p_grupo_id,
		p_hora_ini, p_hora_fin, p_tipo_fin, p_fecha_ini_nuevo
	) AS t2 ON t2.agenda_id <> t1.agenda_id AND (COALESCE(t1.persona_id, 0) = COALESCE(t2.persona_id, 0))
	LEFT JOIN sbsep.view_persona vp ON vp.id = t1.persona_id
	LEFT JOIN sbstar.intervalo intv ON intv.id = t1.intervalo_id
	LEFT JOIN sbstar.horario hora ON hora.id = intv.horario_id
	WHERE ((t1.inicio_planificado, t1.fin_planificado) OVERLAPS(t2.inicio_planificado, t2.fin_planificado))
	AND (vp.estado ISNULL OR vp.estado <> 0)
	AND (t1.agenda_id <> p_agenda)
	AND (t2.agenda_id <> p_agenda)
	ORDER BY fecha_cruce, vp.nombre_completo::VARCHAR DESC;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;
--
DROP FUNCTION sbstar.up_agenda_traer_for_marcacionmovil_by_agenda_id(p_agenda_ids varchar);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_traer_for_marcacionmovil_by_agenda_id (
	IN p_agenda_ids  varchar
)
RETURNS TABLE 
(
	agenda_id bigint,
	estado smallint,
	inicio_planificado timestamp WITH TIME ZONE,
	fin_planificado timestamp WITH TIME ZONE,
	ini_plan_lim_antes timestamp WITH TIME ZONE,
	ini_plan_lim_despues timestamp WITH TIME ZONE,
	fin_plan_lim_antes timestamp WITH TIME ZONE,
	fin_plan_lim_despues timestamp WITH TIME ZONE,
	inicio_real timestamp WITH TIME ZONE,
	fin_real timestamp WITH TIME ZONE,
	limite_antes_inicio interval,
	limite_despues_inicio interval,
	limite_antes_fin interval,
	limite_despues_fin interval,
	marcacion_inicio_habilitado boolean,
	marcacion_fin_habilitado boolean,
	inicio_registrado_por smallint,
	fin_registrado_por smallint,
	intervalo_id bigint,
	intervalo_nombre varchar,
	persona_id bigint,
	horario_id bigint,
	color varchar,
	inicio_real_sincronizado boolean,
	fin_real_sincronizado boolean,
	es_global smallint,
	desde_he_inicio timestamp WITH TIME ZONE,
	hasta_he_inicio timestamp WITH TIME ZONE,
	desde_he_fin timestamp WITH TIME ZONE,
	hasta_he_fin timestamp WITH TIME ZONE
)
AS
$$
DECLARE
BEGIN
	RETURN QUERY
	SELECT DISTINCT
		age.id AS agenda_id,
		age.estado,
		age.inicio_planificado,
		age.fin_planificado,
		age.inicio_planificado - (itr.inicio_limite_antes || ' minutes')::INTERVAL,
		age.inicio_planificado + (itr.inicio_limite_despues || ' minutes')::INTERVAL,
		age.fin_planificado - (itr.fin_limite_antes || ' minutes')::INTERVAL,
		age.fin_planificado + (itr.fin_limite_despues || ' minutes')::INTERVAL,
		age.inicio_real,
		age.fin_real,
		(itr.inicio_limite_antes || ' minutes')::INTERVAL,
		(itr.inicio_limite_despues || ' minutes')::INTERVAL,
		(itr.fin_limite_antes || ' minutes')::INTERVAL,
		(itr.fin_limite_despues || ' minutes')::INTERVAL,
		itr.inicio_marcacion_movil,
		itr.fin_marcacion_movil,
		age.inicio_registrado_por,
		age.fin_registrado_por,
		age.intervalo_id,
		itr.nombre AS intervalo_nombre,
		age.persona_id,
		itr.horario_id,
		itr.color,
		(CASE WHEN age.inicio_registrado_por = 1 THEN TRUE ELSE FALSE END) AS inicio_real_sincronizado,
		(CASE WHEN age.fin_registrado_por = 1 THEN TRUE ELSE FALSE END) AS fin_real_sincronizado,
		age.es_global,
		CASE 
			WHEN itr.inicio_tiempo_extra_desde > 0 THEN
				age.inicio_planificado - itr.inicio_tiempo_extra_desde * INTERVAL '1 SECOND'
			ELSE
				age.inicio_planificado
		END AS desde_he_inicio,
		CASE 
			WHEN itr.inicio_tiempo_extra_hasta > 0 THEN
				age.inicio_planificado - itr.inicio_tiempo_extra_hasta * INTERVAL '1 SECOND'					
			WHEN itr.inicio_limite_antes > 0 THEN
				age.inicio_planificado - itr.inicio_limite_antes * INTERVAL '1 SECOND'
			ELSE
				age.fin_planificado - INTERVAL '24 HOURS'
		END AS hasta_he_inicio,
		CASE 
			WHEN itr.fin_tiempo_extra_desde > 0 THEN
				age.fin_planificado + itr.fin_tiempo_extra_desde * INTERVAL '1 SECOND'
			ELSE age.fin_planificado
		END AS desde_he_fin,
		CASE 
			WHEN itr.fin_tiempo_extra_hasta > 0 THEN
				age.fin_planificado + itr.fin_tiempo_extra_hasta * INTERVAL '1 SECOND'
			WHEN itr.fin_limite_despues > 0 THEN
				age.fin_planificado + itr.fin_limite_despues * INTERVAL '1 SECOND'
			ELSE age.inicio_planificado + INTERVAL '24 HOURS'
		END AS hasta_he_fin
	FROM sbstar.agenda age
	LEFT JOIN sbstar.intervalo itr ON itr.id = age.intervalo_id
	LEFT JOIN sbstar.horario hr ON hr.id = itr.horario_id
	WHERE (age.id::TEXT IN (SELECT UNNEST (string_to_array(p_agenda_ids, ','))) OR p_agenda_ids = '');
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;