import os
import glob
import pandas as pd

# ================= CONFIGURACIÓN =================
BASE_DIR = r"C:\Users\PabloPueo\Downloads\YOIZEN\Nuevo Proyecto - Dash\Logs CLARO Json\BaseJSON"

# NUEVA RUTA DE SALIDA
OUTPUT_DIR = os.path.join(BASE_DIR, "fLog_Maestro_Final")
os.makedirs(OUTPUT_DIR, exist_ok=True) # Crea la carpeta si no existe
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "fLog_Maestro_Final.csv")

COLUMNAS_MAESTRO = [
    "TABLA", "Timestamp", "Fecha_Para_Relacion", "Fecha_Visual", 
    "CaseId", "UserId", "MessageId", "Bloque", "Concepto", 
    "Valor_Detalle", "Estado", "Intervalo", "Contexto", 
    "Request", "Response", "Request_Tooltip", "Response_Tooltip", 
    "Motivo_Error", "Tipo_Error"
]

# ================= FUNCIONES DE MAPEO POR TABLA =================

def procesar_navegacion(df):
    df_out = pd.DataFrame(columns=COLUMNAS_MAESTRO)
    df_out["Timestamp"] = df["Timestamp_Original_UTC"]
    
    df_out["TABLA"] = "1_Navegacion"
    df_out["Fecha_Para_Relacion"] = df["Fecha_Real"] 
    df_out["Fecha_Visual"] = df["Fecha_Real"]        
    df_out["CaseId"] = df["CaseId"]
    df_out["UserId"] = df["UserId"]
    df_out["MessageId"] = df["MessageId"]
    df_out["Bloque"] = df["Detalle"]
    df_out["Concepto"] = df["Evento"] 
    df_out["Valor_Detalle"] = df["Detalle"]
    df_out["Estado"] = df["Level"]
    df_out["Intervalo"] = df["Intervalo_30min"]
    df_out["Contexto"] = df["Contexto"]
    return df_out

def procesar_integraciones(df):
    df_out = pd.DataFrame(columns=COLUMNAS_MAESTRO)
    df_out["Timestamp"] = df["Timestamp_Original_UTC"]
    
    df_out["TABLA"] = "2_Integraciones"
    df_out["Fecha_Para_Relacion"] = df["Fecha_Real"]
    df_out["Fecha_Visual"] = df["Fecha_Real"]
    df_out["CaseId"] = df["CaseId"]
    df_out["UserId"] = df["UserId"]
    df_out["MessageId"] = df["MessageId"]
    df_out["Bloque"] = df["Bloque"]
    df_out["Concepto"] = df["Integracion"]
    df_out["Valor_Detalle"] = df["Duracion_ms"].fillna(0).astype(int).astype(str) + " ms"
    df_out["Estado"] = df["Status"]
    df_out["Intervalo"] = df["Intervalo_30min"]
    df_out["Contexto"] = df["Contexto"]
    df_out["Request"] = df["Request_Raw"]
    df_out["Response"] = df["Response_Raw"]
    df_out["Request_Tooltip"] = df["Request_Legible"]
    df_out["Response_Tooltip"] = df["Response_Legible"]
    df_out["Motivo_Error"] = df["Motivo_Error"]
    df_out["Tipo_Error"] = df["Tipo_Error"]
    return df_out

def procesar_contexto(df):
    df_out = pd.DataFrame(columns=COLUMNAS_MAESTRO)
    df_out["Timestamp"] = df["Timestamp_Original_UTC"]
    
    df_out["TABLA"] = "4_Contexto"
    df_out["Fecha_Para_Relacion"] = df["Fecha_Real"]
    df_out["Fecha_Visual"] = df["Fecha_Real"]
    df_out["CaseId"] = df["CaseId"]
    df_out["UserId"] = df["UserId"]
    df_out["MessageId"] = df["MessageId"]
    df_out["Bloque"] = "Sin Registro de Bloque"
    df_out["Concepto"] = df["Variable"]
    df_out["Valor_Detalle"] = df["Valor"]
    df_out["Estado"] = "Info"
    df_out["Intervalo"] = df["Intervalo_30min"]
    df_out["Contexto"] = df["Contexto"]
    return df_out

def procesar_errores(df):
    df_out = pd.DataFrame(columns=COLUMNAS_MAESTRO)
    df_out["Timestamp"] = df["Timestamp_Original_UTC"]
    
    df_out["TABLA"] = "5_Errores_Motor"
    df_out["Fecha_Para_Relacion"] = df["Fecha_Real"]
    df_out["Fecha_Visual"] = df["Fecha_Real"]
    df_out["CaseId"] = df["CaseId"]
    df_out["UserId"] = df["UserId"]
    df_out["MessageId"] = df["MessageId"]
    df_out["Bloque"] = "Error de Sistema"
    df_out["Concepto"] = df["Error_Type"]
    df_out["Valor_Detalle"] = df["Detalle"]
    df_out["Estado"] = "Error Crítico"
    df_out["Intervalo"] = df["Intervalo_30min"]
    df_out["Contexto"] = "Fallo interno de Yoizen"
    df_out["Motivo_Error"] = df["Detalle"]
    df_out["Tipo_Error"] = df["Error_Type"]
    return df_out

# EXCLUIMOS "3_Mensajes" del diccionario de mapeo
MAPEO_CARPETAS = {
    "1_Navegacion": procesar_navegacion,
    "2_Integraciones": procesar_integraciones,
    "4_Contexto": procesar_contexto,
    "5_Errores_Motor": procesar_errores
}

# ================= PROCESO PRINCIPAL =================

def main():
    print("--- INICIANDO CONSOLIDACIÓN fLog_Maestro ---")
    
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE) 
        print(f"Borrando versión anterior de {os.path.basename(OUTPUT_FILE)}...")

    es_primer_chunk = True
    total_filas = 0

    for carpeta, funcion_mapeo in MAPEO_CARPETAS.items():
        ruta_carpeta = os.path.join(BASE_DIR, carpeta)
        
        if not os.path.exists(ruta_carpeta):
            continue

        archivos_csv = glob.glob(os.path.join(ruta_carpeta, "*.csv"))
        if not archivos_csv:
            continue

        print(f"Procesando {carpeta} ({len(archivos_csv)} archivos)...")

        for archivo in archivos_csv:
            try:
                df_crudo = pd.read_csv(archivo, sep=';', encoding='utf-8-sig', low_memory=False)
                if df_crudo.empty:
                    continue
                
                df_transformado = funcion_mapeo(df_crudo)
                df_transformado.fillna("", inplace=True)
                
                df_transformado.to_csv(OUTPUT_FILE, mode='a', sep=';', index=False, 
                                       encoding='utf-8-sig', header=es_primer_chunk)
                
                es_primer_chunk = False
                total_filas += len(df_transformado)
                
            except Exception as e:
                print(f"  [ERROR] Fallo al procesar {os.path.basename(archivo)}: {e}")

    print(f"\n--- CONSOLIDACIÓN FINALIZADA ---")
    print(f"Tabla de Hechos generada con éxito: {total_filas:,} filas.")
    print(f"Archivo listo en: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()