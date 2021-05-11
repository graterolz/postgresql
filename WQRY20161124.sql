DROP FUNCTION sbspla.up_planillaperiodo_calculo_descarga(
	p_planilla_id bigint,
	p_periodo_id bigint,
	p_cant_decimales integer
);
--
CREATE OR REPLACE FUNCTION sbspla.up_planillaperiodo_calculo_descarga (
	IN p_planilla_id bigint,
	IN p_periodo_id bigint,
	IN p_cant_decimales integer
)
RETURNS TABLE 
(
	nombre_completo text,
	valor numeric
)
AS
$$
BEGIN
	RETURN QUERY 
	SELECT
		vp.nombre_completo,
		TO_CHAR(COALESCE(pval.valor, 0), rpad('FM9999999990.', p_cant_decimales + 13, '0'))::NUMERIC AS valor
	FROM sbspla.planillapersonagrupo ppg
	INNER JOIN sbspla.contratoplanilla cp ON ppg.contrato_id = cp.id AND cp.estado = 1
	INNER JOIN sbsep.view_persona vp ON cp.persona_id = vp.id AND vp.estado = 1
	INNER JOIN sbspla.planillavalor pval ON ppg.id = pval.planillapersonagrupo_id AND pval.estado = 1
	INNER JOIN sbspla.planillavariable pvar ON pval.planillavariable_id = pvar.id AND pvar.estado = 1
	INNER JOIN sbssys.enumeracion enu ON pvar.variable_id = enu.id AND enu.estado = 1 AND enu.id = 495
	WHERE ppg.planilla_id = p_planilla_id
	AND ppg.periodo_id = p_periodo_id
	AND pval.valor > 0
	ORDER BY vp.nombre_completo;
END
$$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;