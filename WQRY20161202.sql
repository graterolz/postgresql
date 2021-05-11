DROP FUNCTION IF EXISTS sbstar.up_periodoagenda_traer_periodo_x_empresa(
	p_empresa_id bigint, p_tipoperiodo_id bigint
);
--
CREATE OR REPLACE FUNCTION sbstar.up_periodoagenda_traer_periodo_x_empresa (
	IN p_empresa_id bigint,
	IN p_tipoperiodo_id bigint
)
RETURNS TABLE 
(
  anio varchar,
  periodos text
)
AS
$$
DECLARE
	periodicidad VARCHAR;
BEGIN 
	SELECT en.codigo INTO periodicidad
	FROM sbssys.enumeracion en
	INNER JOIN sbstar.tipoperiodo tp ON tp.id = p_tipoperiodo_id
	WHERE en.id = tp.tipo_id
	LIMIT 1;
	--
	IF periodicidad = 'tpa_mes' THEN
		RETURN QUERY
		SELECT
			tbl.ejercicio as anio,
			'[' || PUBLIC.agr_fila_unir (
				'{ "fecha_inicio":"' || to_char(tbl.inicio, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT ||
				'", "fecha_fin":"' || to_char(tbl.fin, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT || 
				'", "nombre":"' || tbl.nombre :: TEXT || 
				'", "id": "' || tbl.id :: TEXT || '"}'
			) || ']' as periodos
		FROM (
			SELECT pa.id, pa.ejercicio, pa.inicio, pa.nombre, pa.fin
			FROM sbstar.periodoagenda pa
			WHERE estado = 1
			AND empresa_id = p_empresa_id
			AND tipoperiodo_id = p_tipoperiodo_id
			ORDER BY ejercicio DESC, inicio ASC
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
			SELECT
				tblChild.id,pa.ejercicio,pa.inicio,pa.nombre,pa.fin,tblChild.children as children
			FROM sbstar.periodoagenda pa
			LEFT JOIN (
				SELECT
					tblpa.padre_id as id,
					'[' || PUBLIC.agr_fila_unir (
						'{ "fecha_inicio":"' || to_char(tblpa.inicio, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT ||
						'", "fecha_fin":"' || to_char(tblpa.fin, 'dd/MM/yyyy hh12:mi:ss am') :: TEXT||
						'", "nombre":"' || tblpa.nombre :: TEXT ||
						'", "id": "' || tblpa.id :: TEXT || '"}'
					) || ']' as children
				FROM (
					SELECT
						pa.padre_id,pa.id,pa.ejercicio, pa.inicio,pa.nombre,pa.fin
					FROM sbstar.periodoagenda pa
					WHERE estado = 1
					AND empresa_id = p_empresa_id
					AND tipoperiodo_id = p_tipoperiodo_id
					AND pa.padre_id IS NOT NULL
					ORDER BY pa.ejercicio DESC, pa.inicio ASC
				) tblpa
				GROUP BY tblpa.padre_id
			) tblChild ON pa.id = tblChild.id
			WHERE pa.estado = 1
			AND pa.empresa_id = p_empresa_id
			AND pa.tipoperiodo_id = p_tipoperiodo_id
			AND pa.padre_id IS NULL
			ORDER BY pa.ejercicio DESC, pa.inicio ASC
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