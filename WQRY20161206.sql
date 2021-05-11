DROP FUNCTION sbstar.up_periodoagenda_traer_childrens_x_empresa (
	p_empresa_id bigint,
	p_tipoperiodo_id bigint
);
--
CREATE OR REPLACE FUNCTION sbstar.up_periodoagenda_traer_childrens_x_empresa (
	IN p_empresa_id bigint,
	IN p_tipoperiodo_id bigint
)
RETURNS TABLE 
(
	padre_id bigint,
	id bigint,
	ejercicio text,
	inicio date,
	nombre varchar,
	fin date
)
AS
$$
DECLARE
	periodicidad VARCHAR;
BEGIN 
	RETURN QUERY
	SELECT 
		pa.padre_id,
		pa.id,
		pa.ejercicio,
		pa.inicio,
		pa.nombre,
		pa.fin,
		COALESCE(pa.estado_cierre ,0) as estado_cierre
	FROM sbstar.periodoagenda pa
	WHERE pa.estado = 1 
	AND	pa.empresa_id = p_empresa_id
	AND	pa.tipoperiodo_id = p_tipoperiodo_id 
	AND	pa.padre_id IS NOT NULL;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;
--
DROP FUNCTION sbspla.up_periodo_cerrar (
	p_periodo_id bigint
);
--
CREATE OR REPLACE FUNCTION sbspla.up_periodo_cerrar (
	IN p_periodo_id bigint
)
RETURNS BOOLEAN
AS
$$
DECLARE
	v_count numeric;
	v_padre_id bigint;
	v_count_children int;
	v_count_children_cerrado int;
	v_actualiza_padre boolean := false;
BEGIN
	SELECT COUNT(*) INTO v_count
	FROM sbstar.periodoagenda
	WHERE COALESCE(estado_cierre, 0) = 0
	AND estado = 1
	AND id = p_periodo_id;
	--	
	IF(v_count > 0) THEN
		SELECT padre_id INTO v_padre_id
		FROM sbstar.periodoagenda
		WHERE estado = 1
		AND id = p_periodo_id;
		--
		IF (v_padre_id IS NOT NULL) THEN
			SELECT COUNT(*) INTO v_count_children
			FROM sbstar.periodoagenda
			WHERE padre_id = v_padre_id;
			--			
			SELECT COUNT(*) INTO v_count_children_cerrado
			FROM sbstar.periodoagenda
			WHERE padre_id = v_padre_id
			AND estado_cierre = 1;
			--			
			v_count_children_cerrado := v_count_children_cerrado + 1;
			--
			IF(v_count_children = v_count_children_cerrado) THEN
				v_actualiza_padre:= true;
			END IF;	
		END IF;
		--
		UPDATE sbstar.periodoagenda
		SET estado_cierre = 1
		WHERE id = p_periodo_id;
		--
		IF(v_actualiza_padre = true) THEN
			UPDATE sbstar.periodoagenda
			SET estado_cierre = 1
			WHERE id = v_padre_id;
		END IF;
		--
		UPDATE sbstar.tiemporesumen
		SET estado = 1
		WHERE id = p_periodo_id;
		--
		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;
--
DROP FUNCTION sbspla.up_periodo_abrir (
	p_periodo_id bigint
);
--
CREATE OR REPLACE FUNCTION sbspla.up_periodo_abrir (
	IN p_periodo_id bigint
)
RETURNS BOOLEAN
AS
$$
DECLARE
	v_count numeric;
	v_padre_id bigint;
	v_count_children int;
	v_count_children_abierto int;
	v_actualiza_padre boolean := false;
BEGIN
	SELECT COUNT(*) INTO v_count
	FROM sbstar.periodoagenda
	WHERE COALESCE(estado_cierre, 0) = 1
	AND id = p_periodo_id;
	--	
	IF(v_count > 0) THEN
		SELECT padre_id INTO v_padre_id
		FROM sbstar.periodoagenda
		WHERE estado = 1
		AND id = p_periodo_id;
		--		
		IF (v_padre_id IS NOT NULL) THEN
			SELECT COUNT(*) INTO v_count_children
			FROM sbstar.periodoagenda
			WHERE padre_id = v_padre_id;
			--
			SELECT COUNT(*) INTO v_count_children_abierto
			FROM sbstar.periodoagenda
			WHERE padre_id = v_padre_id
			AND estado_cierre = 0;
			--
			v_count_children_abierto := v_count_children_abierto + 1;
			--
			IF(v_count_children = v_count_children_abierto) THEN
				v_actualiza_padre:= true;
			END IF;
		END IF;
		--		
		UPDATE sbstar.periodoagenda
		SET estado_cierre = 0
		WHERE id = p_periodo_id;
		--
		IF(v_actualiza_padre = true) THEN
			UPDATE sbstar.periodoagenda
			SET estado_cierre = 0
			WHERE id = v_padre_id;
		END IF;
		--		
		UPDATE sbstar.tiemporesumen
		SET estado = 1
		WHERE id = p_periodo_id;
		--		
		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;