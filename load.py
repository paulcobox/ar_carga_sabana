import pandas as pd
import pyodbc
import logging
from datetime import datetime
import mysql.connector

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='ar_carga_sabana.log',
    filemode='a'
)

logger = logging.getLogger(__name__)

def validar_precio(valor):
    """Valida el campo PRECIO y devuelve (valor_limpio, mensaje)"""
    textos_a_reemplazar = ["ANULADO", "BONIFICADO", "BONIFICADO ANULADO", "BONIFICADO PERDIDO", "BONIFICADO  PERDIDO"]
    
    # Guardar valor original para mensajes
    valor_original = str(valor)
    
    if isinstance(valor, str):
        valor = valor.strip().upper()
        # Verificar si contiene textos a reemplazar
        for texto in textos_a_reemplazar:
            if texto in valor:
                return 0.0, f"Reemplazado por 0 (contiene '{texto}')"
        
        # Limpiar símbolos de moneda
        valor_limpio = valor.replace('S/', '').replace(' ', '').replace(',', '')
    else:
        valor_limpio = str(valor)
    
    try:
        precio = float(valor_limpio) if pd.notna(valor_limpio) else 0.0
        if precio < 0:
            return 0.0, "Reemplazado por 0 (valor negativo)"
        return precio, "OK"
    except (ValueError, TypeError):
        return 0.0, f"Reemplazado por 0 (valor no numérico: '{valor_original}')"
    
    
def limpiar_precio(valor):
    """Limpia y convierte el campo PRECIO"""
    textos_a_reemplazar = ["ANULADO", "BONIFICADO", "BONIFICADO ANULADO"]
    
    if isinstance(valor, str):
        valor = valor.strip().upper()
        if any(texto in valor for texto in textos_a_reemplazar):
            return 0.0
        # Eliminar símbolos de moneda, espacios y comas
        valor = valor.replace('S/', '').replace(' ', '').replace(',', '')
    
    try:
        return float(valor) if pd.notna(valor) else 0.0
    except (ValueError, TypeError):
        return 0.0

def limpiar_texto(valor):
    """Limpia campos de texto eliminando espacios extras y convirtiendo a mayúsculas"""
    if isinstance(valor, str):
        return valor.strip().upper()
    return valor

def validar_codigo(codigo):
    """Valida el formato del código (ej: 0-35-13-3203-012025-100) y devuelve (bool, str)"""
    if not isinstance(codigo, str):
        return False, "No es una cadena de texto"
    
    partes = codigo.split('-')
    
    if len(partes) != 6:
        return False, f"Debe tener 5 guiones (tiene {len(partes)-1})"
    
    if not all(p.isdigit() for p in partes):
        return False, "Todas las partes deben ser numéricas"
    
    if len(partes[4]) != 6:
        return False, "La parte 5 debe tener 6 dígitos (año-mes)"
    
    if not codigo[-1].isdigit():
        return False, "Debe terminar con un dígito"
    
    return True, "Válido"

def validar_mes(mes):
    """Valida que el mes sea válido (1-12 o nombre de mes)"""
    meses_validos = {
        'ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 
        'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'SETIEMBRE',
        'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE'
    }
    
    if isinstance(mes, str):
        return mes.strip().upper() in meses_validos
    elif isinstance(mes, (int, float)):
        return 1 <= int(mes) <= 12
    return False

def obtener_info_registro(row):
    """Obtiene información identificadora del registro"""
    return f"CODIGO: {row.get('CODIGO', 'N/A')} | ASESOR: {row.get('ASESOR', 'N/A')} | PROYECTO: {row.get('PROYECTO', 'N/A')}"

def limpiar_y_validar_dataframe(df):
    """Realiza la limpieza y validación completa del DataFrame"""
    # 1. Limpieza inicial de datos
    logger.info("Iniciando limpieza de datos...")
    
    try:
         # Lista para almacenar registros con precios modificados
        precios_modificados = []
        
        # Aplicar validación y obtener mensajes
        resultados = df['PRECIO'].apply(lambda x: validar_precio(x))
        df['PRECIO_LIMPIO'] = [r[0] for r in resultados]
        df['PRECIO_MENSAJE'] = [r[1] for r in resultados]
        
        # Obtener registros con precios modificados
        df_modificados = df[df['PRECIO_MENSAJE'] != "OK"].copy()
        
        # Procesar cada registro modificado para el reporte
        for idx, row in df_modificados.iterrows():
            precios_modificados.append({
                'Fila': idx + 2,
                'PRECIO_ORIGINAL': row['PRECIO'],
                'PRECIO_LIMPIO': row['PRECIO_LIMPIO'],
                'Motivo': row['PRECIO_MENSAJE'],
                'CODIGO': row['CODIGO'],
                'ASESOR': row['ASESOR']
            })
        
        # Mostrar todos los registros con precios modificados en el log
        if precios_modificados:
            logger.warning("REGISTROS CON PRECIOS MODIFICADOS:")
            for registro in precios_modificados:
                logger.warning(
                    f"Fila {registro['Fila']}: "
                    f"PRECIO_ORIGINAL='{registro['PRECIO_ORIGINAL']}' | "
                    f"PRECIO_LIMPIO={registro['PRECIO_LIMPIO']} | "
                    f"Motivo: {registro['Motivo']} | "
                    f"CODIGO: {registro['CODIGO']} | "
                    f"ASESOR: {registro['ASESOR']}"
                )
            logger.warning(f"Total de registros con precios modificados: {len(precios_modificados)}")
        
        # Reemplazar la columna PRECIO con los valores limpios
        df['PRECIO'] = df['PRECIO_LIMPIO']
        df = df.drop(columns=['PRECIO_LIMPIO', 'PRECIO_MENSAJE'])
        
        
       
        # Limpiar campos de texto y asegurar formato uniforme
        columnas_texto = ['ASESOR', 'INMOBILIARIA', 'SERVICIO', 'PROYECTO', 'DISTRITO', 'LIMA QUE PERTENECE', 'CODIGO']
        for col in columnas_texto:
            df[col] = df[col].astype(str).str.strip().str.upper()
        
        # Convertir años a numérico
        df['AÑO DE FACTURACIÓN'] = pd.to_numeric(df['AÑO DE FACTURACIÓN'], errors='coerce')
        df['AÑO DE REALIZACIÓN'] = pd.to_numeric(df['AÑO DE REALIZACIÓN'], errors='coerce')
        
    except Exception as e:
        logger.error(f"Error durante la limpieza de datos: {str(e)}")
        raise ValueError("Error en la limpieza de datos") from e
    
    # 2. Filtrar datos y registrar códigos inválidos
    logger.info("Filtrando datos y validando códigos...")
    try:
        # Primero filtramos solo registros de 2025
        df_2025 = df[
            (df['AÑO DE FACTURACIÓN'].notna()) & 
            (df['AÑO DE FACTURACIÓN'] == 2025)
        ].copy()
        total_2025 = len(df_2025)
        
        # Lista para almacenar registros con códigos inválidos
        registros_invalidos = []
        
        # Validar cada código en registros de 2025 y recolectar los inválidos
        df_2025['CODIGO_VALIDO'] = df_2025['CODIGO'].apply(lambda x: validar_codigo(x)[0])
        
        # Obtener registros con códigos inválidos (solo de 2025)
        df_invalidos = df_2025[~df_2025['CODIGO_VALIDO']].copy()
        df_validos = df_2025[df_2025['CODIGO_VALIDO']].copy()
        
        # Procesar cada registro inválido para el reporte
        for idx, row in df_invalidos.iterrows():
            _, motivo = validar_codigo(row['CODIGO'])
            registros_invalidos.append({
                'Fila': idx + 2,  # +2 porque Excel comienza en 1 y la fila 1 es encabezado
                'CODIGO': row['CODIGO'],
                'Motivo': motivo,
                'ASESOR': row['ASESOR'],
                'PROYECTO': row['PROYECTO'],
                'AÑO FACTURACIÓN': row['AÑO DE FACTURACIÓN']
            })
        
        # Mostrar todos los registros inválidos en el log
        if registros_invalidos:
            logger.warning("REGISTROS DE 2025 CON CÓDIGOS INVÁLIDOS QUE NO SERÁN INSERTADOS:")
            for registro in registros_invalidos:
                logger.warning(
                    f"Fila {registro['Fila']}: CODIGO='{registro['CODIGO']}' | "
                    f"Motivo: {registro['Motivo']} | "
                    f"ASESOR: {registro['ASESOR']} | "
                    f"PROYECTO: {registro['PROYECTO']} | "
                    f"AÑO: {registro['AÑO FACTURACIÓN']}"
                )
            # Reporte resumen
            logger.info(
                f"REPORTE DE VALIDACIÓN 2025:\n"
                f"  - Total de registros del 2025: {total_2025}\n"
                f"  - Registros válidos: {len(df_validos)}\n"
                f"  - Registros inválidos: {len(df_invalidos)}"
            )
        
        # Filtrar solo registros válidos (de 2025 con código válido)
        df_filtrado = df_2025[df_2025['CODIGO_VALIDO']].copy()
        
        # Agregar registros de otros años (sin validar código)
        otros_anios = df[df['AÑO DE FACTURACIÓN'] != 2025]
        df_final = pd.concat([df_filtrado, otros_anios])
        
        logger.info(f"Datos filtrados correctamente. Filas validas para 2025: {len(df_filtrado)} | Otros años: {len(otros_anios)}")
        
    except KeyError as e:
        logger.error(f"Error: La columna {e} no existe en el archivo Excel.")
        raise
    except Exception as e:
        logger.error(f"Error inesperado al filtrar datos: {str(e)}")
        raise
    
    # 3. Validación de datos filtrados
    logger.info("Validando datos filtrados...")
    errores = []
    
    for idx, row in df_filtrado.iterrows():
        registro_info = obtener_info_registro(row)
        errores_registro = []
        
        # Validar campos obligatorios
        campos_requeridos = ['ASESOR', 'INMOBILIARIA', 'TIPO', 'SERVICIO', 'PROYECTO', 'DISTRITO', 'LIMA QUE PERTENECE']
        for campo in campos_requeridos:
            if not isinstance(row[campo], str) or not row[campo].strip():
                errores_registro.append(f"{campo} inválido (vacío o no texto)")
        
        # Validar meses
        # Validar MES DE FACTURACIÓN
        if pd.isna(row['MES DE FACTURACIÓN']):
            errores_registro.append("MES DE FACTURACIÓN está vacío")
        elif not validar_mes(row['MES DE FACTURACIÓN']):
            errores_registro.append(f"MES DE FACTURACIÓN inválido: {row['MES DE FACTURACIÓN']}")

        # Validar MES REALIZACIÓN
        if pd.isna(row['MES REALIZACIÓN']):
            errores_registro.append("MES REALIZACIÓN está vacío")
        elif not validar_mes(row['MES REALIZACIÓN']):
            errores_registro.append(f"MES REALIZACIÓN inválido: {row['MES REALIZACIÓN']}")

        
        # Validar precios
        if not isinstance(row['PRECIO'], (int, float)) or row['PRECIO'] < 0:
            errores_registro.append(f"PRECIO inválido: {row['PRECIO']}")
        
        if errores_registro:
            errores.append(f"\nREGISTRO CON ERRORES - {registro_info}:\n" + 
                         "\n".join(f"  - {error}" for error in errores_registro))
    
    if errores:
        error_msg = "Errores de validación encontrados:" + "\n".join(errores[:5])
        if len(errores) > 5:
            error_msg += f"\n  ... y {len(errores)-5} registros más con errores"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    return df_filtrado

def transformar_meses(df):
    """Transforma los nombres de meses a números"""
    meses_a_numeros = {
        'ENERO': 1, 'FEBRERO': 2, 'MARZO': 3, 'ABRIL': 4, 'MAYO': 5, 
        'JUNIO': 6, 'JULIO': 7, 'AGOSTO': 8, 
        'SEPTIEMBRE': 9, 'SETIEMBRE': 9,
        'OCTUBRE': 10, 'NOVIEMBRE': 11, 'DICIEMBRE': 12
    }
    
    for col in ['MES DE FACTURACIÓN', 'MES REALIZACIÓN']:
        df[col] = df[col].apply(
            lambda x: meses_a_numeros.get(str(x).strip().upper(), x) if isinstance(x, str) else x
        )
    return df
def safe_date(valor):
    """Convierte una fecha en formato válido a objeto datetime.date. Si no es válida, retorna None."""
    if pd.isna(valor):
        return None
    if isinstance(valor, datetime):
        return valor.date()
    try:
        return pd.to_datetime(valor, errors='coerce').date()
    except Exception:
        return None
    
def main():
    conn = None
    try:
        # 1. Leer archivo Excel
        # archivo_excel = r'C:\Users\pcruces\Desktop\apps\ar_carga_sabana\SABANA_FULL_JUNIO_2.xlsx'
        archivo_excel = r'C:\Users\pcruces\Desktop\apps\ar_carga_sabana\SABANA_FULL_17_JULIO.xlsx'
        logger.info(f"Cargando archivo Excel: {archivo_excel}")
        df = pd.read_excel(archivo_excel)
        
        # 2. Limpiar, filtrar y validar datos
        df_limpio = limpiar_y_validar_dataframe(df)
        
        # 3. Transformar meses a números
        df_final = transformar_meses(df_limpio)
        
        # 4. Conexión a BD e inserción
        logger.info("Conectando a la base de datos...")
        # conn = pyodbc.connect(
        #     "DRIVER={ODBC Driver 18 for SQL Server};"
        #     "SERVER=asei.database.windows.net;"
        #     "DATABASE=PowerBI_ASEI;"
        #     "UID=aluna@asei;"
        #     "PWD=Server2015;"
        #     "Encrypt=yes;"
        #     "TrustServerCertificate=no;"
        #     "Connection Timeout=30;"
        # )
        
        conn = mysql.connector.connect(
            host="35.231.96.40",
            port=3307,
            user="u_ezavala",
            password="uathie3eis8iligiThef",  # reemplaza esto con la contraseña real
            database="db_nexo_staging"
        )
        
        cursor = conn.cursor()
        conn.autocommit = False
        
        try:
            logger.info(f"Iniciando inserción de {len(df_final)} registros...")
            for _, row in df_final.iterrows():
                registro_info = obtener_info_registro(row)
                try:
                    cursor.execute('''
                        INSERT INTO AR_SABANA_FULL (
                            ASESOR, MES_FACTURACION, ANO_FACTURACION, TIPO, PRECIO, CODIGO,
                            INMOBILIARIA, SERVICIO, PROYECTO, DISTRITO, LIMA_QUE_PERTENECE,
                            MES_REALIZACION, ANO_REALIZACION,
                            FB_INST, FECHA_FB_INST,
                            MAILING, FECHA_MAILING,
                            DESTACADO_NORMAL, FECHA_INICIO_DESTACADO_NORMAL, FECHA_FIN_DESTACADO_NORMAL,
                            REMARKETING, FECHA_REMARKETING,
                            BANNER_TOP, FECHA_INICIO_BANNER_TOP, FECHA_FIN_BANNER_TOP,
                            TOMA_DE_CANAL, FECHA_INICIO_TOMA_DE_CANAL, FECHA_FIN_TOMA_DE_CANAL,
                            WSP_NEXO_EVENTO, FECHA_WSP_NEXO_EVENTO
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        row['ASESOR'],
                        int(row['MES DE FACTURACIÓN']),
                        int(row['AÑO DE FACTURACIÓN']),
                        row['TIPO'],
                        float(row['PRECIO']),
                        row['CODIGO'].strip(),
                        row['INMOBILIARIA'],
                        row['SERVICIO'],
                        row['PROYECTO'],
                        row['DISTRITO'],
                        row['LIMA QUE PERTENECE'],
                        int(row['MES REALIZACIÓN']),
                        int(row['AÑO DE REALIZACIÓN']),
                        row.get('FB_INST'),
                        safe_date(row.get('FECHA_FB_INST')),
                        row.get('MAILING'),
                        safe_date(row.get('FECHA_MAILING')),
                        row['DESTACADO_NORMAL'],
                        safe_date(row['FECHA_INICIO_DESTACADO_NORMAL']),
                        safe_date(row['FECHA_FIN_DESTACADO_NORMAL']),
                        row.get('REMARKETING'),
                        safe_date(row.get('FECHA_REMARKETING')),
                        row['BANNER_TOP'],
                        safe_date(row['FECHA_INICIO_BANNER_TOP']),
                        safe_date(row['FECHA_FIN_BANNER_TOP']),
                        row['TOMA_DE_CANAL'],
                        safe_date(row['FECHA_INICIO_TOMA_DE_CANAL']),
                        safe_date(row['FECHA_FIN_TOMA_DE_CANAL']),
                        row.get('WSP_NEXO_EVENTO'),
                        safe_date(row.get('FECHA_WSP_NEXO_EVENTO'))
                    ))


                    logger.debug(f"Registro insertado: {registro_info}")
                except Exception as e:
                    logger.error(f"Error al insertar registro {registro_info}: {str(e)}")
                    raise
            
            conn.commit()
            logger.info("Inserción completada exitosamente")
            
        except Exception as e:
            conn.rollback()
            logger.error("Error durante la inserción. Se realizó rollback.")
            raise ValueError("No se insertó ningún registro debido a errores") from e
            
    except ValueError as ve:
        logger.error(f"Error de validación:\n{str(ve)}")
        raise
    except Exception as e:
        logger.error(f"Error en el proceso: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Conexión a la base de datos cerrada")

if __name__ == "__main__":
    main()