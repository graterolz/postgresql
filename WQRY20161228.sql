DROP FUNCTION sbstar.up_agenda_traer_con_excepciones_agrupado_x_intervalo_x_dia(
	p_fecha_inicio timestamp WITH TIME ZONE,
	p_fecha_fin timestamp WITH TIME ZONE,
	p_horario_id bigint,
	p_intervalo_id text,
	p_grupo_id bigint,
	p_persona_id text
);
--
CREATE OR REPLACE FUNCTION sbstar.up_agenda_traer_con_excepciones_agrupado_x_intervalo_x_dia (
	IN p_fecha_inicio timestamp WITH TIME ZONE,
	IN p_fecha_fin timestamp WITH TIME ZONE,
	IN p_horario_id bigint,
	IN p_intervalo_id text,
	IN p_grupo_id bigint,
	IN p_persona_id text
)
RETURNS TABLE
(
	nrodia text,
	descripcion text,
	intervalos varchar,
	marcas varchar
)
AS
$$
DECLARE
BEGIN
	RETURN QUERY
	SELECT
		to_char(dias.fecha_inicio,'yyyymmdd'),
		to_char(dias.fecha_inicio,'dd/mm/yyyy'),
		('[' || public.agr_fila_unir(
			'{'||
				'"turno":' || '"' || COALESCE(dataAge.turno,'') || '"'||
				',"titulo":' || '"' || COALESCE(dataAge.titulo,'') || '"'|| 
				',"horaIni":' || '"' || COALESCE(dataAge.horaIni::TEXT,'') || '"' ||  
				',"minutoIni":' || '"' || COALESCE(dataAge.minutoIni::TEXT,'') || '"' || 
				',"horaFin":' || '"' || COALESCE(dataAge.horaFin::TEXT,'') || '"' || 
				',"minutoFin":' || '"' || COALESCE(dataAge.minutoFin::TEXT,'') || '"' || 
				',"horaRealIni":' || '"' || COALESCE(dataAge.horaRealIni::TEXT,'') || '"' || 
				',"minutoRealIni":' || '"' || COALESCE(dataAge.minutoRealIni::TEXT,'') || '"' || 
				',"horaRealFin":' || '"' || COALESCE(dataAge.horaRealFin::TEXT,'') || '"' || 
				',"minutoRealFin":' || '"' || COALESCE(dataAge.minutoRealFin::TEXT,'') || '"' || 
				',"agenda_id":' || COALESCE(dataAge.agenda_id,0) || 
				',"intervalo_id":' || '"' || COALESCE(dataAge.intervalo_id,0) || '"'|| 
				',"intervalo_nombre":' || '"' || COALESCE(dataAge.intervalo_nombre,'') || '"'|| 
				',"intervalo_color":' || '"' || COALESCE(dataAge.intervalo_color,'') || '"'|| 
				',"intervalo_codigo":' || '"' || COALESCE(dataAge.intervalo_codigo,'') || '"'|| 
				',"intervalo_tipo_fin":' || '"' || COALESCE(dataAge.intervalo_tipo_fin,'') || '"'|| 
				',"agenda_inicio_planificado":' || '"' || COALESCE(dataAge.agenda_inicio_planificado::TEXT,'') || '"'|| 
				',"agenda_fin_planificado":' || '"' || COALESCE(dataAge.agenda_fin_planificado::TEXT,'') || '"'|| 
				',"agenda_inicio_real":' || '"' || COALESCE(TO_CHAR(EXTRACT(HOUR FROM dataAge.agenda_inicio_real),'FM00') || ':' || TO_CHAR(EXTRACT(MINUTE FROM dataAge.agenda_inicio_real),'FM00'),'') || '"'|| 
				',"agenda_fin_real":' || '"' || COALESCE(TO_CHAR(EXTRACT(HOUR FROM dataAge.agenda_fin_real),'FM00') || ':' || TO_CHAR(EXTRACT(MINUTE FROM dataAge.agenda_fin_real),'FM00'),'') || '"'||
				',"agenda_horas":' || '"' || COALESCE(public.fn_intervalo_en_horas((dataAge.agenda_fin_planificado-dataAge.agenda_inicio_planificado))::VARCHAR,'') || '"'||
				',"es_reprogramada":' || dataAge.es_reprogramada || 
				',"es_cerrada":' || dataAge.es_cerrada || 
				',"es_vacaciones":' || dataAge.es_vacaciones ||
				',"intervalo_inicio_req_marca":' || COALESCE(dataAge.intervalo_inicio_req_marca, 0) || 
				',"intervalo_fin_req_marca":' || COALESCE(dataAge.intervalo_fin_req_marca, 0) || 
				',"horario_id":' || COALESCE(dataAge.horario_id, 0) || 
				',"horario_nombre":' || '"' || COALESCE(dataAge.horario_nombre::TEXT,'') || '"' || 
				',"excepcion_permite_marcacion":' || dataAge.excepcion_permite_marcacion || 
				',"excepciones":' || COALESCE(dataAge.excepciones,'') || 
			'}') || 
		']')::VARCHAR AS intervalos,
		('[' || public.agr_fila_unir(DISTINCT
			'{'||
				'"turno":' || '"' || COALESCE(dataMarca.turno,'') || '"'||
				',"titulo":' || '"' || COALESCE(dataMarca.titulo,'') || '"'|| 
				',"horaRealIni":' || '"' || COALESCE(dataMarca.horaRealIni::TEXT,'') || '"' || 
				',"minutoRealIni":' || '"' || COALESCE(dataMarca.minutoRealIni::TEXT,'') || '"' || 
				',"horaRealFin":' || '"' || COALESCE(dataMarca.horaRealFin::TEXT,'') || '"' ||
				',"minutoRealFin":' || '"' || COALESCE(dataMarca.minutoRealFin::TEXT,'') || '"' || 
				',"agenda_id":' || dataMarca.agenda_id || 
				',"intervalo_id":' || '"' || COALESCE(dataMarca.intervalo_id,0)	 || '"'|| 
				',"intervalo_nombre":' || '"' || COALESCE(dataMarca.intervalo_nombre,'')	 || '"'|| 
				',"intervalo_color":' || '"' || COALESCE(dataMarca.intervalo_color,'')		 || '"'|| 
				',"intervalo_codigo":' || '"' || COALESCE(dataMarca.intervalo_codigo,'')	 || '"'|| 
				',"intervalo_tipo_fin":' || '"' || COALESCE(dataMarca.intervalo_tipo_fin,'')	 || '"'|| 
				',"intervalo_inicio_req_marca":' || COALESCE(dataMarca.intervalo_inicio_req_marca, 0) || 
				',"intervalo_fin_req_marca":' || COALESCE(dataMarca.intervalo_fin_req_marca, 0)		|| 
				',"agenda_inicio_real":' || '"' || COALESCE(TO_CHAR(EXTRACT(HOUR FROM dataMarca.agenda_inicio_real),'FM00') || ':' || TO_CHAR(EXTRACT(MINUTE FROM dataMarca.agenda_inicio_real),'FM00'),'') || '"'|| 
				',"agenda_fin_real":' || '"' || COALESCE(TO_CHAR(EXTRACT(HOUR FROM dataMarca.agenda_fin_real),'FM00') || ':' || TO_CHAR(EXTRACT(MINUTE FROM dataMarca.agenda_fin_real),'FM00'),'') || '"'||
			'}') || 
		']')::VARCHAR AS marcas
	FROM sbstar.up_agenda_traer_con_excepciones_agrupado_x_intervalo_json(
		p_fecha_inicio, p_fecha_fin, p_horario_id, p_intervalo_id, p_grupo_id, p_persona_id
	) AS dataAge
	RIGHT JOIN sbsrpt.up_genera_intervalos_tiempo(
		to_char(p_fecha_inicio,'yyyy-mm-dd')::timestamp WITH TIME ZONE,
		to_char(p_fecha_fin,'yyyy-mm-dd 23:59:59')::timestamp WITH TIME ZONE,
		'dia','es_pe') AS dias
	ON dataAge.und_tiempo_id=to_char(dias.fecha_inicio,'yyyymmdd')
	LEFT JOIN sbstar.up_marca_traer_base(
		p_fecha_inicio,p_fecha_fin,p_horario_id,p_intervalo_id,p_grupo_id, p_persona_id
	) AS dataMarca
	ON dataMarca.und_tiempo_id=to_char(dias.fecha_inicio,'yyyymmdd')
	GROUP BY to_char(dias.fecha_inicio,'yyyymmdd'),to_char(dias.fecha_inicio,'dd/mm/yyyy')
	ORDER BY to_char(dias.fecha_inicio,'yyyymmdd'),to_char(dias.fecha_inicio,'dd/mm/yyyy');
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;