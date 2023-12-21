drop table if exists proceso.obligaciones_clientes_final purge;

CREATE table proceso.obligaciones_clientes_final stored as PARQUET as
WITH base_tasa AS
(
	SELECT
	t1.radicado, t1.num_documento, t1.cod_segm_tasa, t2.segmento, t1.cod_subsegm_tasa, t1.cal_interna_tasa,
	t1.id_producto, t1.tipo_id_producto, 
	CASE
	WHEN lower(t1.id_producto) like '%operacion_especifica%' THEN 'Operacion especifica'
	WHEN lower(t1.id_producto) like '%leasing%' THEN 'Leasing'
	WHEN lower(t1.id_producto) like '%sufi%' THEN 'Sufi'
	WHEN lower(t1.id_producto) like '%factoring%' THEN 'Factoring'
	WHEN lower(t1.id_producto) like '%tarjeta%' THEN 'Tarjeta'
	WHEN lower(t1.id_producto) like '%hipotecario%' THEN 'Hipotecario'
	WHEN lower(t1.id_producto) like '%cartera%' THEN 'Cartera'
	ELSE 'Otros'
	END producto,
	t1.valor_inicial, t1.fecha_desembolso, t1.plazo,
	t1.cod_periodicidad, t1.periodicidad, t1.saldo_deuda, t1.modalidad, t1.tipo_plazo,
	CASE
	WHEN lower(t1.id_producto) like '%operacion_especifica%' THEN t2.tasa_operacion_especifica
	WHEN lower(t1.id_producto) like '%leasing%' THEN t2.tasa_leasing
	WHEN lower(t1.id_producto) like '%sufi%' THEN t2.tasa_sufi
	WHEN lower(t1.id_producto) like '%factoring%' THEN t2.tasa_factoring
	WHEN lower(t1.id_producto) like '%tarjeta%' THEN t2.tasa_tarjeta
	WHEN lower(t1.id_producto) like '%hipotecario%' THEN t2.tasa_hipotecario
	WHEN lower(t1.id_producto) like '%cartera%' THEN t2.tasa_cartera
	ELSE 0
	END tasa
	FROM proceso.obligaciones_clientes AS t1
	INNER JOIN proceso.tasas_productos AS t2 on
	t1.cod_segm_tasa = t2.cod_segmento and t1.cod_subsegm_tasa = t2.cod_subsegmento
	and t1.cal_interna_tasa = t2.calificacion_riesgos
),
base_efectiva AS
(
	SELECT
	t1.radicado, t1.num_documento, t1.cod_segm_tasa, t1.segmento, t1.cod_subsegm_tasa, t1.cal_interna_tasa,
	t1.id_producto, t1.tipo_id_producto, t1.producto, t1.valor_inicial, t1.fecha_desembolso, t1.plazo,
	t1.cod_periodicidad, t1.periodicidad, t1.saldo_deuda, t1.modalidad, t1.tipo_plazo,
	t1.tasa, power((1+t1.tasa),(1/(12/t1.cod_periodicidad))) - 1 AS tasa_efectiva
	FROM base_tasa AS t1
)
	SELECT
	t1.radicado, t1.num_documento, t1.cod_segm_tasa, t1.segmento, t1.cod_subsegm_tasa, t1.cal_interna_tasa,
	t1.id_producto, t1.tipo_id_producto, t1.producto, t1.valor_inicial, t1.fecha_desembolso, t1.plazo,
	t1.cod_periodicidad, t1.periodicidad, t1.saldo_deuda, t1.modalidad, t1.tipo_plazo,
	t1.tasa, t1.tasa_efectiva, t1.tasa_efectiva * t1.valor_inicial AS valor_final
	FROM base_efectiva AS t1
;


drop table if exists proceso.obligaciones_clientes_unico purge;
CREATE table proceso.obligaciones_clientes_unico stored as PARQUET as
SELECT
t1.num_documento, sum(t1.valor_final) valor_final, count(t1.valor_final) productos
FROM proceso.obligaciones_clientes_final AS t1
group by t1.num_documento
having count(t1.valor_final) > 1;