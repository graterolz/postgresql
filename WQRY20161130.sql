DROP FUNCTION sbspla.up_agenda_traer_por_compensar (
	p_periodo_id bigint,
	p_persona_id bigint,
	p_grupo_id bigint
);
--
CREATE OR REPLACE FUNCTION sbspla.up_agenda_traer_por_compensar (
	IN p_periodo_id bigint,
	IN p_persona_id bigint,
	IN p_grupo_id  bigint
)
RETURNS TABLE
(
	nombre varchar,
	ejercicio varchar,
	ersona_id bigint,
	por_compensar numeric,
	exc_contra_por_compensar float
)
AS
$$
BEGIN
	IF (p_persona_id IS NULL OR LENGTH(TRIM(p_persona_id::varchar)) = 0) THEN
	RETURN QUERY 
	SELECT
		per.nombre, 
		per.ejercicio, 
		age.persona_id, 
		COALESCE(SUM(age.por_compensar),0) por_compensar,
		public.fn_intervalo_en_segundos(SUM(
			LEAST(age.fin_planificado,exc.fecha_hora_fin,age.fin_real) - 
			GREATEST(age.inicio_planificado,exc.fecha_hora_inicio,age.inicio_real)
		)) exc_contra_por_compensar
	FROM sbstar.excepcion exc 
	INNER JOIN sbstar.configuracionexcepcion cexp ON exc.configuracionexcepcion_id = cexp.id AND cexp.estado = 1
	INNER JOIN sbstar.tipoexcepcion texc ON cexp.tipoexcepcion_id = texc.id AND texc.estado = 1
	INNER JOIN sbssys.enumeracion enu ON texc.tipo_id = enu.id 
	RIGHT JOIN sbstar.agenda age ON (
		(exc.fecha_hora_inicio<=age.inicio_planificado AND exc.fecha_hora_fin>=age.fin_planificado) OR
		(exc.fecha_hora_inicio<=age.inicio_planificado AND exc.fecha_hora_fin>=age.inicio_planificado) OR
		(exc.fecha_hora_inicio<=age.fin_planificado AND exc.fecha_hora_fin>=age.fin_planificado) OR
		(exc.fecha_hora_inicio>=age.inicio_planificado AND exc.fecha_hora_fin<=age.fin_planificado) AND		 
		(((p_persona_id::varchar='' AND p_grupo_id=0) AND (cexp.es_global=1)) OR
		((p_grupo_id<>0 AND p_persona_id::varchar='') AND (cexp.es_global=1 OR cexp.grupo_id=p_grupo_id)) OR
		((p_persona_id::varchar<>'' AND p_grupo_id=0) AND (cexp.es_global=1 OR cexp.persona_id::text IN (
			SELECT unnest(string_to_array(p_persona_id::varchar,','))) OR
			cexp.grupo_id IN (
				SELECT grpPe.grupo_id FROM sbsep.grupopersona grpPe
				WHERE grpPe.persona_id::varchar IN (
					SELECT unnest(string_to_array(p_persona_id::varchar,','))
				) AND grpPe.estado = 1
			)
		)))
	)
	INNER JOIN sbstar.periodoagenda per ON (
		(per.inicio<=age.inicio_planificado AND per.fin>=age.fin_planificado) OR
		(per.inicio<=age.inicio_planificado AND per.fin>=age.inicio_planificado) OR
		(per.inicio<=age.fin_planificado AND per.fin>=age.fin_planificado) OR
		(per.inicio>=age.inicio_planificado AND per.fin<=age.fin_planificado)
	)
	WHERE age.estado = 1
	AND age.persona_id IS NOT NULL
	AND age.es_global = 0
	AND per.estado = 1
	AND enu.clasificacion = 'TipoExcepcion' 
	AND enu.codigo = 'en_contra_por_compensar'
	AND enu.estado = 1
	AND per.id = p_periodo_id
	GROUP BY per.nombre, per.ejercicio, age.persona_id
	ORDER BY 1 DESC;
ELSE
	RETURN QUERY
	SELECT
		per.nombre, 
		per.ejercicio,
		age.persona_id, 
		COALESCE(SUM(age.por_compensar),0) por_compensar,
		public.fn_intervalo_en_segundos(SUM(
			LEAST(age.fin_planificado,exc.fecha_hora_fin,age.fin_real) - 
			GREATEST(age.inicio_planificado,exc.fecha_hora_inicio,age.inicio_real)
		)) exc_contra_por_compensar
	FROM sbstar.excepcion exc 
	INNER JOIN sbstar.configuracionexcepcion cexp ON exc.configuracionexcepcion_id = cexp.id AND cexp.estado = 1
	INNER JOIN sbstar.tipoexcepcion texc ON cexp.tipoexcepcion_id = texc.id AND texc.estado = 1
	INNER JOIN sbssys.enumeracion enu ON texc.tipo_id = enu.id 
	RIGHT JOIN sbstar.agenda age ON (
		(exc.fecha_hora_inicio<=age.inicio_planificado AND exc.fecha_hora_fin>=age.fin_planificado) OR
		(exc.fecha_hora_inicio<=age.inicio_planificado AND exc.fecha_hora_fin>=age.inicio_planificado) OR
		(exc.fecha_hora_inicio<=age.fin_planificado AND exc.fecha_hora_fin>=age.fin_planificado) OR
		(exc.fecha_hora_inicio>=age.inicio_planificado AND exc.fecha_hora_fin<=age.fin_planificado) AND		 
		(((p_persona_id::varchar='' AND p_grupo_id=0) AND (cexp.es_global=1)) OR
		((p_grupo_id<>0 AND p_persona_id::varchar='') AND (cexp.es_global=1 OR cexp.grupo_id=p_grupo_id)) OR
		((p_persona_id::varchar<>'' AND p_grupo_id=0) AND (cexp.es_global=1 OR cexp.persona_id::varchar IN (
			SELECT unnest(string_to_array(p_persona_id::text,','))) OR
			cexp.grupo_id IN (
				SELECT grpPe.grupo_id FROM sbsep.grupopersona grpPe
				WHERE grpPe.persona_id::varchar IN (
					SELECT unnest(string_to_array(p_persona_id::varchar,','))
				) AND grpPe.estado=1
			)
		)))
	)
	INNER JOIN sbstar.periodoagenda per ON (
		(per.inicio<=age.inicio_planificado AND per.fin>=age.fin_planificado) OR
		(per.inicio<=age.inicio_planificado AND per.fin>=age.inicio_planificado) OR
		(per.inicio<=age.fin_planificado AND per.fin>=age.fin_planificado) OR
		(per.inicio>=age.inicio_planificado AND per.fin<=age.fin_planificado)
	)
	WHERE age.estado = 1 AND age.persona_id IS NOT NULL AND age.es_global = 0 AND per.estado = 1
	AND enu.clasificacion = 'TipoExcepcion' 
	AND enu.codigo = 'en_contra_por_compensar'
	AND enu.estado = 1
	AND per.id = p_periodo_id
	AND age.persona_id = p_persona_id
	GROUP BY per.nombre, per.ejercicio, age.persona_id
	ORDER BY 1 DESC;
END IF;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;