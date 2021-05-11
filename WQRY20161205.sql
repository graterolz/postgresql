DROP FUNCTION sbspla.up_periodo_cerrar (
	p_periodo_id bigint,
	p_estado_cierre bigint,
	p_usuariocreador_id bigint
);
--
CREATE OR REPLACE FUNCTION sbspla.up_periodo_cerrar (
	IN p_periodo_id bigint,
	IN p_estado_cierre bigint,
	IN p_usuariocreador_id bigint
)
RETURNS BOOLEAN
AS
$$
DECLARE
	v_persona_id bigint;
	v_por_compensar numeric;
	v_exc_contra_por_compensar numeric;

	v_cursor CURSOR FOR
		SELECT * FROM sbspla.up_agenda_traer_por_compensar (p_periodo_id,NULL,0);
	cur_row RECORD;
	--
	v_periodos_cerrados int;
	v_tipoperiodo_id bigint;
	v_ejercicio varchar;
	v_cantidad_periodo_cerrado bigint;
	v_periodo_id_anterior bigint;
	v_por_compensar_anterior numeric;
	v_exc_contra_por_compensar_anterior float;
	v_saldo_anterior numeric;
BEGIN		
	IF p_estado_cierre = 1 THEN
		UPDATE sbstar.tiemporesumen
		SET estado = 1
		WHERE id = p_periodo_id;
		--
		UPDATE sbstar.periodoagenda
		SET estado_cierre = 1
		WHERE id = p_periodo_id;		
	ELSIF p_estado_cierre = 0 THEN		
		SELECT tipoperiodo_id,ejercicio INTO v_tipoperiodo_id,v_ejercicio
		FROM sbstar.periodoagenda
		WHERE id = p_periodo_id
		AND padre_id IS NULL;
		--
		SELECT COUNT(*) INTO v_cantidad_periodo_cerrado
		FROM sbstar.periodoagenda
		WHERE padre_id IS NULL
		AND tipoperiodo_id = v_tipoperiodo_id
		AND ejercicio = v_ejercicio
		AND estado_cierre = 1
		AND id <> p_periodo_id;
		--
		IF (v_cantidad_periodo_cerrado > 0) THEN
			SELECT id INTO v_periodo_id_anterior
			FROM sbstar.periodoagenda
			WHERE padre_id IS NULL
			AND tipoperiodo_id = v_tipoperiodo_id
			AND ejercicio = v_ejercicio
			AND (estado_cierre IS NULL OR estado_cierre = 1)
			AND id <> p_periodo_id
			ORDER BY 1 DESC
			LIMIT 1;
			--
			OPEN v_cursor;
			LOOP
			FETCH v_cursor INTO cur_row;
			EXIT WHEN NOT FOUND;
				v_persona_id = cur_row.persona_id;
				v_por_compensar = cur_row.por_compensar;
				v_exc_contra_por_compensar = cur_row.exc_contra_por_compensar;
				--
				SELECT por_compensar,exc_contra_por_compensar
				INTO v_por_compensar, v_exc_contra_por_compensar
				FROM sbspla.up_agenda_traer_por_compensar (p_periodo_id,v_persona_id,0);
				--
				SELECT COALESCE(saldo, 0)::NUMERIC INTO v_saldo_anterior
				FROM sbstar.tiemporesumen
				WHERE periodoagenda_id = v_periodo_id_anterior
				AND persona_id = v_persona_id;
				--
				IF(v_saldo_anterior ISNULL) THEN
					v_saldo_anterior:= 0;
				END IF;
				--
				v_saldo_anterior := (v_por_compensar - v_exc_contra_por_compensar) + v_saldo_anterior;
				--
				INSERT INTO sbstar.tiemporesumen (
					periodoagenda_id, persona_id, usuariocreador_id, usuarioeditor_id, por_compensar,
					exc_contra_por_compensar, fecha_creacion, fecha_edicion, estado, saldo
				)
				VALUES (
					p_periodo_id, v_persona_id, p_usuariocreador_id, NULL, v_por_compensar, 
					v_exc_contra_por_compensar,current_timestamp, current_timestamp, 1, v_saldo_anterior
				);
			END LOOP;
			CLOSE v_cursor;
			--
			UPDATE sbstar.periodoagenda
			SET estado_cierre = p_estado_cierre
			WHERE id = p_periodo_id;
		ELSE
			UPDATE sbstar.periodoagenda
			SET estado_cierre = p_estado_cierre
			WHERE id = p_periodo_id;
		END IF;
	END IF;
	RETURN TRUE;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;