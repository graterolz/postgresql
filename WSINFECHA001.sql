--20161104
SELECT * FROM sbspla.up_convenios_traer_disponibles('',2,3,'1','5','nombre','desc');
--20161130 
SELECT * FROM sbspla.up_agenda_traer_por_compensar(2290,2235,0);
--20161123
SELECT * FROM sbspla.up_retencionesjudiciales_traer_data('',NULL,'1','10','','');
--20161124
SELECT * FROM sbspla.up_planillaperiodo_calculo_descarga(162,2273,2);
--20161206
SELECT * FROM sbspla.up_periodo_cerrar(4269);
--
UPDATE sbstar.periodoagenda SET estado_cierre = NULL;
DELETE FROM sbstar.tiemporesumen;
--
SELECT * FROM sbssys.enumeracion WHERE clasificacion = 'AgendaTipoPeriodo'
SELECT * FROM sbstar.tipoperiodo WHERE id = 2290;
SELECT * FROM sbstar.periodoagenda WHERE id = 2290;
SELECT * FROM sbstar.tiemporesumen WHERE periodoagenda_id = 2290;
SELECT * FROM sbspla.up_periodo_calcular(2290,NULL);
SELECT * FROM sbspla.up_periodo_calcular(2292,NULL);
SELECT * FROM sbsep.persona LIMIT 10;
SELECT * FROM sbstar.horario LIMIT 10;
SELECT * FROM sbstar.personahorario LIMIT 10;
SELECT * FROM sbstar.intervalo where id = 1357
SELECT * FROM sbstar.agenda LIMIT 10;
SELECT * FROM sbstar.up_agenda_traer_base_con_tiempos_procesados_y_excp(
	'2016-12-05 19:00'::TIMESTAMP,'2016-12-06 05:00'::TIMESTAMP,0,'','','2989'
);
SELECT * FROM sbstar.up_agenda_calcular_tiempos_to_save(
	'2016-12-05 19:00'::TIMESTAMP,'2016-12-06 05:00'::TIMESTAMP,0,'','','2989'
);
--
SELECT * FROM sbstar.up_agenda_reprogramacion_valida_traslapes(
	'2016-11-27 10:00:00'::TIMESTAMP,'2016-11-27 14:00:00'::TIMESTAMP,0,2895,0,'10:00',NULL,'mismo_dia',0,'2016-11-25 00:00:00',5198384
);
SELECT * FROM sbstar.up_agenda_reprogramacion_valida_traslapes_virtuales(
	'2016-12-10 10:00:00'::TIMESTAMP,'2016-12-10 14:00:00'::TIMESTAMP,1105,2895,0,'10:00',NULL,'mismo_dia',NULL,'2016-12-11 00:00:00',5198384
);
SELECT * FROM sbstar.up_agenda_reprogramacion_simula(
	'2016-12-19 10:00:00'::TIMESTAMP,'2016-12-19 15:00:00'::TIMESTAMP,1088,2895,0,'10:00',NULL,'mismo_dia','2016-12-20 00:00:00'
);
SELECT * FROM sbstar.up_agenda_traer_x_intervalopadre_y_fechas(
	'2016-12-19 10:00:00'::TIMESTAMP,'2016-12-19 15:00:00'::TIMESTAMP,1088,2895,0
);
SELECT * FROM sbstar.up_agenda_traer_con_excepciones_agrupado_x_intervalo_x_dia(
	'2016-09-01 00:59:59'::TIMESTAMP,'2016-10-01 00:59:59'::TIMESTAMP,0::SMALLINT,'',0::SMALLINT,'2895'
);
SELECT * FROM sbstar.up_agenda_traer_con_excepciones_agrupado_x_intervalo_json(
	'2016-09-01 00:59:59'::TIMESTAMP,'2016-10-01 00:59:59'::TIMESTAMP,0::SMALLINT,'',0::SMALLINT,'2895'
);
--
SET TIME ZONE 'AMERICA/CARACAS';
SELECT * FROM sbstar.up_busca_agendas_traslapes(
	'2016-09-08 10:00:00'::TIMESTAMP,'2016-09-08 14:00:00'::TIMESTAMP,0,2895,0,'13:30',NULL,'mismo_dia',0,'2016-09-09',4692781
);
SELECT * FROM sbstar.up_busca_agendas_traslapes(
	'2016-09-14 10:00:00'::TIMESTAMP,'2016-09-14 14:00:00'::TIMESTAMP,1329,2895,0,'13:30',NULL,'mismo_dia',0,'2016-09-15',0
);
SELECT * FROM sbstar.up_busca_agendas_traslapes(
	'2016-09-08 10:00:00'::TIMESTAMP,'2016-09-08 14:00:00'::TIMESTAMP,0,2895,0,'13:30',NULL,'mismo_dia',0,'2016-09-09',4692781
);
--
UPDATE sbstar.periodoagenda SET estado_cierre = NULL;
SELECT * FROM sbspla.up_periodo_calcular(4269,null);
SELECT * FROM sbspla.up_periodo_cerrar(4269);
SELECT * FROM sbspla.up_periodo_calcular(4270,null);
SELECT * FROM sbspla.up_periodo_cerrar(4270);
SELECT * FROM sbspla.up_periodo_cerrar(2290,0,null);
--
ALTER FUNCTION sbstar.up_agenda_reprogramacion_simula(
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_intervalo_id bigint,
	p_persona_id bigint,
	p_grupo_id bigint,
	p_hora_ini varchar,
	p_hora_fin varchar,
	p_tipo_fin varchar,
	p_fecha_ini_nuevo timestamp WITH TIME ZONE
) OWNER TO egraterol;
--
SELECT * FROM sbsep.up_personadireccion_search_by(2804,'es_pe',null);
--
ALTER FUNCTION sbstar.up_agenda_traer_for_marcacionmovil_by_agenda_id(
	p_agenda_ids varchar
) OWNER TO postgres;
--
ALTER FUNCTION public.up_solicitudsunat_search_by(
	culture varchar,
	empresaid bigint,
	estadobusquedaid smallint,
	texto text,
	pagenumber integer,
	pagesize integer,
	sortfield varchar,
	sortorder varchar
) OWNER TO postgres;
--
ALTER FUNCTION sbstar.up_agenda_reprogramacion_valida_traslapes
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
) OWNER TO egraterol;
--
ALTER FUNCTION sbstar.up_agenda_reprogramacion_valida_traslapes_virtuales(
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
) OWNER TO egraterol;
--
ALTER FUNCTION sbsep.up_personas_traer_x_permisos(
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
) OWNER TO postgres;