import numpy as np
import pandas

#Cargar ambos conjuntos de datos en Python utilizando pandas.
obligaciones = pandas.read_excel("Parte 2/Obligaciones_clientes.xlsx", sheet_name="Obligaciones_clientes")
tasas = pandas.read_excel("Parte 2/tasas_productos.xlsx", sheet_name="Tasas")

#Fusionar ambos conjuntos de datos en función de las columnas relevantes.
datos = pandas.merge(obligaciones,tasas,\
                    left_on=['cod_segm_tasa','cod_subsegm_tasa','cal_interna_tasa'],\
                    right_on=['cod_segmento','cod_subsegmento','calificacion_riesgos'],how="inner")

datos['fecha_desembolso'] = pandas.to_datetime(datos['fecha_desembolso'])

#Tomamos la tasa correspondiente de acuerdo  al tipo de producto
datos['tasa'] = np.where(datos['id_producto'].str.lower().str.contains('operacion_especifica'), datos['tasa_operacion_especifica'],
                    np.where(datos['id_producto'].str.lower().str.contains('leasing'), datos['tasa_leasing'],
                    np.where(datos['id_producto'].str.lower().str.contains('sufi'), datos['tasa_sufi'],
                    np.where(datos['id_producto'].str.lower().str.contains('factoring'), datos['tasa_factoring'],
                    np.where(datos['id_producto'].str.lower().str.contains('tarjeta'), datos['tasa_tarjeta'],
                    np.where(datos['id_producto'].str.lower().str.contains('hipotecario'), datos['tasa_hipotecario'],
                    np.where(datos['id_producto'].str.lower().str.contains('cartera'), datos['tasa_cartera'], 0
                    )))))))

#Creamos un campo que nos diferencie mejor el tipo de producto
datos['producto'] = np.where(datos['id_producto'].str.lower().str.contains('operacion_especifica'), 'Operacion_especifica',
                    np.where(datos['id_producto'].str.lower().str.contains('leasing'), 'Leasing',
                    np.where(datos['id_producto'].str.lower().str.contains('sufi'), 'Sufi',
                    np.where(datos['id_producto'].str.lower().str.contains('factoring'), 'Factoring',
                    np.where(datos['id_producto'].str.lower().str.contains('tarjeta'), 'Tarjeta',
                    np.where(datos['id_producto'].str.lower().str.contains('hipotecario'), 'Hipotecario',
                    np.where(datos['id_producto'].str.lower().str.contains('cartera'), 'Cartera', 'Otros'
                    )))))))

# convertimos la tasa a una tasa efectiva
datos['tasa_efectiva'] = np.power((1 + datos['tasa']),(1/(12/datos['cod_periodicidad']))) - 1
# calculamod el valor fina.
datos['valor_final'] = datos['tasa_efectiva'] * datos['valor_inicial']

#Filramos las columnas relevantes 
datos = datos[['radicado', 'num_documento', 'cod_segm_tasa', 'segmento', 'cod_subsegm_tasa', 'cal_interna_tasa',
'id_producto', 'tipo_id_producto', 'producto', 'valor_inicial', 'fecha_desembolso', 'plazo',
'cod_periodicidad', 'periodicidad', 'saldo_deuda', 'modalidad', 'tipo_plazo','tasa',
'tasa_efectiva','valor_final']]

#Sumamos el valor_final de las obligaciones por cliente y selecionamos las 
#que tenga una cantidad de productos mayor o igual a 2
consulta = datos.groupby('num_documento').agg(valor_final=('valor_final', 'sum'),productos=('valor_final', 'count'))
consulta = consulta[consulta['productos'] > 1]

#Exportamos los resultados
datos.to_excel("Parte 2/datos.xlsx", sheet_name='datos',index=False)
consulta.to_excel("Parte 2/consulta.xlsx", sheet_name='consulta',index=True)  