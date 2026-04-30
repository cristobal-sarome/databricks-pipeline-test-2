import json
import pandas as pd
import os
import glob
import re
from datetime import datetime, timedelta

# ================= CONFIGURACIÓN =================
INPUT_FOLDER = r"C:\Users\PabloPueo\Downloads\YOIZEN\Nuevo Proyecto - Dash\Logs CLARO Json\BaseTXTpScript"
OUTPUT_BASE = r"C:\Users\PabloPueo\Downloads\YOIZEN\Nuevo Proyecto - Dash\Logs CLARO Json\BaseJSON"

BATCH_SIZE = 50000 
HOURS_OFFSET = -5 

DIR_NAV = os.path.join(OUTPUT_BASE, "1_Navegacion")
DIR_INT = os.path.join(OUTPUT_BASE, "2_Integraciones")
DIR_MSG = os.path.join(OUTPUT_BASE, "3_Mensajes")
DIR_CTX = os.path.join(OUTPUT_BASE, "4_Contexto")
DIR_ERR = os.path.join(OUTPUT_BASE, "5_Errores_Motor")

for d in [DIR_NAV, DIR_INT, DIR_MSG, DIR_CTX, DIR_ERR]:
    os.makedirs(d, exist_ok=True)

NOMBRES_IGNORAR = {
    "bodyLength", "accessToken", "responseCode", "httpStatus", 
    "url", "method", "timeout", "headers", "claveDerivacion",
    "loop", "index", "counter", "i", "j", "length"
}

PATRON_VARIABLE = re.compile(r"Se estableció el valor de la variable (.+?) al valor obtenido de la expresión (.*)")

# ================= FUNCIONES DE LIMPIEZA =================

def nuking_newlines(text):
    if not text: return ""
    return str(text).replace('\n', ' ').replace('\r', ' ')

def make_pretty_flat(text):
    if not text: return ""
    text = nuking_newlines(text.strip())
    try:
        data = json.loads(text)
        lines = []
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    lines.append(f"{key}:")
                    for k2, v2 in value.items():
                        lines.append(f"   - {k2}: {v2}")
                elif isinstance(value, list):
                    lines.append(f"{key}: {value}")
                else:
                    lines.append(f"{key}: {value}")
            return nuking_newlines(" ||| ".join(lines))
    except:
        pass
    try:
        clean = text
        if clean.startswith('{'): clean = clean[1:]
        if clean.endswith('}'): clean = clean[:-1]
        formatted = clean.replace('","', '" ||| "').replace('", "', '" ||| "').replace("','", "' ||| '")
        return nuking_newlines(formatted)
    except:
        return nuking_newlines(text)

def extract_req_res(text):
    req_raw, res_raw = "", ""
    req_pretty, res_pretty = "", ""
    try:
        text_clean = nuking_newlines(text)
        idx_datos = text_clean.find("con datos ")
        idx_resp = text_clean.find(" y respondió ")
        idx_tiempos = text_clean.find(" en tiempos ")

        if idx_datos != -1 and idx_resp != -1:
            raw = text_clean[idx_datos + 10 : idx_resp]
            req_raw = raw.strip()
            req_pretty = make_pretty_flat(req_raw)
        
        if idx_resp != -1:
            if idx_tiempos != -1:
                raw = text_clean[idx_resp + 13 : idx_tiempos]
            else:
                raw = text_clean[idx_resp + 13 :]
            res_raw = raw.strip()
            res_pretty = make_pretty_flat(res_raw)

        return req_raw, res_raw, req_pretty, res_pretty
    except:
        return "", "", "", ""

def extract_duration(text):
    match = re.search(r'"total":\s*(\d+)', text)
    if match: return int(match.group(1))
    return None

# ================= FUNCIÓN BUSCANDO ERRORES =================
def analizar_status_con_motivo(log_level, response_raw):
    """
    Devuelve una tupla: (Status, Tipo_Error, Motivo_Detallado)
    """
    if log_level and log_level.lower() == 'error':
        return 'Error', 'Error Interno Bot', 'Nivel de Log marcado como Error en Yoizen'
    
    if not response_raw or response_raw.strip() == "" or response_raw.strip() == "null":
        return 'Error', 'Timeout / Sin Respuesta', 'La integración no devolvió respuesta (Posible Timeout)'

    try:
        text = nuking_newlines(response_raw)
        data = json.loads(text)
        
        if isinstance(data, dict):
            
            http_status = None
            if 'httpStatus' in data:
                try: http_status = int(data['httpStatus'])
                except: pass

            # --- ERRORES HTTP (4xx y 5xx) ---
            if http_status:
                if 400 <= http_status < 500:
                    return 'Error', 'Error de Petición (4xx)', f"HTTP Status {http_status}"
                if http_status >= 500:
                    return 'Error', 'Error de Servidor (5xx)', f"HTTP Status {http_status}"

            # --- ERRORES LÓGICOS ---
            if 'responseCode' in data:
                rc = data['responseCode']
                if isinstance(rc, int) and (rc < 200 or rc >= 300):
                    return 'Error', 'Error de Lógica/Negocio', f"responseCode numérico interno: {rc}"
                if isinstance(rc, str) and rc.upper() not in ['OK', '200']:
                    return 'Error', 'Error de Lógica/Negocio', f"responseCode texto interno: {rc}"

            if 'error' in data:
                val = data['error']
                if val is True or (isinstance(val, str) and val.lower() == 'true'):
                    return 'Error', 'Error de Lógica/Negocio', "Campo 'error' es True"
                if isinstance(val, str) and len(val) > 0 and val.lower() not in ["null", "false"]:
                     return 'Error', 'Error de Lógica/Negocio', f"Mensaje API: {val}" 

            if 'isError' in data and data['isError'] is True:
                return 'Error', 'Error de Lógica/Negocio', "Campo 'isError' principal es True"
            
            if 'data' in data and isinstance(data['data'], dict):
                inner = data['data']
                if 'error' in inner and isinstance(inner['error'], dict):
                    if inner['error'].get('isError') is True:
                        return 'Error', 'Error de Lógica/Negocio', "Campo anidado 'data.error.isError' es True"
    except:
        return 'Error', 'Error de Formato', 'La respuesta no es un JSON válido'

    return 'OK', 'Sin Error', ''

def process_time_data(iso_timestamp):
    if not iso_timestamp: return None, None, None
    try:
        ts_clean = iso_timestamp.replace('Z', '')[:23] 
        try:
            dt_utc = datetime.fromisoformat(ts_clean)
        except ValueError:
             dt_utc = datetime.fromisoformat(ts_clean.split('.')[0])
        dt_local = dt_utc + timedelta(hours=HOURS_OFFSET)
        fecha_real = dt_local.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        dia_real = dt_local.strftime('%Y-%m-%d')
        minute_floor = 0 if dt_local.minute < 30 else 30
        intervalo = f"{dt_local.hour:02d}:{minute_floor:02d}"
        return fecha_real, dia_real, intervalo
    except:
        return None, None, None

def save_batch(data_list, folder, base_filename, suffix, is_first_batch):
    if not data_list: return
    df = pd.DataFrame(data_list)
    output_path = os.path.join(folder, f"{base_filename}_{suffix}.csv")
    mode = 'w' if is_first_batch else 'a'
    header = True if is_first_batch else False
    df.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig', mode=mode, header=header, quoting=1)

def safe_json_parse(line):
    if not line.startswith('{'): return None
    try:
        return json.loads(line)
    except:
        return None

# ================= PROCESAMIENTO =================

def process_file(file_path):
    nombre_archivo = os.path.basename(file_path)
    base_name = os.path.splitext(nombre_archivo)[0]
    print(f"--> Procesando: {nombre_archivo} (V26 - Descubrimientos de Auditoría)...")
    
    data_nav, data_int, data_msg, data_ctx, data_err = [], [], [], [], []
    contexto_bloques = {}
    
    first_batch_nav = True
    first_batch_int = True
    first_batch_msg = True
    first_batch_ctx = True
    first_batch_err = True

    buffer_line = ""

    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line: continue

                if line.startswith('{'):
                    if buffer_line:
                        process_single_log(buffer_line, nombre_archivo, data_nav, data_int, data_msg, data_ctx, data_err, contexto_bloques)
                    buffer_line = line
                else:
                    buffer_line += " " + line

                if buffer_line.endswith('}'):
                     process_single_log(buffer_line, nombre_archivo, data_nav, data_int, data_msg, data_ctx, data_err, contexto_bloques)
                     buffer_line = "" 
                
                # Guardados por lotes
                if len(data_nav) >= BATCH_SIZE: save_batch(data_nav, DIR_NAV, base_name, "nav", first_batch_nav); data_nav=[]; first_batch_nav=False
                if len(data_int) >= BATCH_SIZE: save_batch(data_int, DIR_INT, base_name, "int", first_batch_int); data_int=[]; first_batch_int=False
                if len(data_msg) >= BATCH_SIZE: save_batch(data_msg, DIR_MSG, base_name, "msg", first_batch_msg); data_msg=[]; first_batch_msg=False
                if len(data_ctx) >= BATCH_SIZE: save_batch(data_ctx, DIR_CTX, base_name, "ctx", first_batch_ctx); data_ctx=[]; first_batch_ctx=False
                if len(data_err) >= BATCH_SIZE: save_batch(data_err, DIR_ERR, base_name, "err", first_batch_err); data_err=[]; first_batch_err=False

            if buffer_line:
                process_single_log(buffer_line, nombre_archivo, data_nav, data_int, data_msg, data_ctx, data_err, contexto_bloques)

        # Guardado final de remanentes
        save_batch(data_nav, DIR_NAV, base_name, "nav", first_batch_nav)
        save_batch(data_int, DIR_INT, base_name, "int", first_batch_int)
        save_batch(data_msg, DIR_MSG, base_name, "msg", first_batch_msg)
        save_batch(data_ctx, DIR_CTX, base_name, "ctx", first_batch_ctx)
        save_batch(data_err, DIR_ERR, base_name, "err", first_batch_err)
        print(f"   [OK] Archivo finalizado.")

    except Exception as e:
        print(f"   [ERROR] {nombre_archivo}: {e}")

def process_single_log(line_text, nombre_archivo, data_nav, data_int, data_msg, data_ctx, data_err, contexto_bloques):
    log = safe_json_parse(line_text)
    if not log: return

    timestamp = log.get('time')
    level = log.get('level')
    msg = nuking_newlines(log.get('msg', ''))
    
    payload = log.get('payload', {})
    if not isinstance(payload, dict): payload = {}

    case_id = payload.get('caseId')
    user_id = payload.get('userId')
    msg_id = payload.get('messageId')

    if not case_id: return

    fecha_real, dia_real, intervalo = process_time_data(timestamp)

    # 1. NAVEGACIÓN Y SALUD DEL FLUJO (V27)
    if "Bloque actual:" in msg:
        try: bloque = msg.split("Bloque actual:")[-1].strip()
        except: bloque = "Desconocido"
        
        contexto_bloques[case_id] = bloque
        data_nav.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id,
            'MessageId': msg_id, 'Evento': 'Paso por Bloque', 'Detalle': bloque, 
            'Level': level, 'Origen_Archivo': nombre_archivo, 'Contexto': f"Bloque actual: {bloque}" 
        })
    elif "Se continuará la ejecución en el bloque" in msg:
        # V26: Nuevo evento de salto capturado
        try: bloque = msg.split("ejecución en el bloque")[-1].strip()
        except: bloque = "Desconocido"
        contexto_bloques[case_id] = bloque # Actualizamos contexto
        data_nav.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id,
            'MessageId': msg_id, 'Evento': 'Salto a Bloque', 'Detalle': bloque, 
            'Level': level, 'Origen_Archivo': nombre_archivo, 'Contexto': msg 
        })
    elif "ya fue visitado en esta ejecución" in msg and "Se detiene la ejecución" in msg:
        data_nav.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id,
            'MessageId': msg_id, 'Evento': 'Bucle Detectado', 'Detalle': 'El sistema detuvo la ejecución por seguridad', 
            'Level': level, 'Origen_Archivo': nombre_archivo, 'Contexto': msg 
        })
    elif "Inicio logueo de nuevo caso" in msg or "El caso es nuevo, iniciamos" in msg:
        data_nav.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id,
            'MessageId': msg_id, 'Evento': 'Inicio de Caso', 'Detalle': 'Apertura de caso/sesión', 
            'Level': level, 'Origen_Archivo': nombre_archivo, 'Contexto': msg 
        })
    elif "Terminó el procesamiento del flujo" in msg:
        data_nav.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id,
            'MessageId': msg_id, 'Evento': 'Fin de Flujo', 'Detalle': 'Cierre normal del procesamiento', 
            'Level': level, 'Origen_Archivo': nombre_archivo, 'Contexto': msg 
        })
    elif "No se encontro ninguna opcion del menu que coincida" in msg:
        # V26: Captura de input inválido (Fallback)
        data_nav.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id,
            'MessageId': msg_id, 'Evento': 'Input Inválido (Fallback)', 'Detalle': 'El usuario ingresó una opción no válida', 
            'Level': level, 'Origen_Archivo': nombre_archivo, 'Contexto': msg 
        })

    # 2. INTEGRACIONES
    elif "Se invocó a la integración" in msg or "El servicio =>" in msg:
        bloque_origen = contexto_bloques.get(case_id, "Desconocido")
        nom_int, req_raw, res_raw, req_pretty, res_pretty = "Indefinida", "", "", "", ""
        
        if "Se invocó a la integración" in msg:
            try: nom_int = msg.split("integración")[1].split("con datos")[0].strip()
            except: pass
            req_raw, res_raw, req_pretty, res_pretty = extract_req_res(msg)
            
        else:
            try: nom_int = msg.split("El servicio =>")[1].split("Para los datos")[0].strip()
            except: pass
            
            idx_datos = msg.find("Para los datos =>")
            idx_devuelve = msg.find("Devuelve =>")
            if idx_datos != -1 and idx_devuelve != -1:
                req_raw = msg[idx_datos + 17 : idx_devuelve].strip()
                res_raw = msg[idx_devuelve + 11 :].strip()
                req_pretty = make_pretty_flat(req_raw)
                res_pretty = make_pretty_flat(res_raw)
        
        status_real, tipo_error, motivo_error = analizar_status_con_motivo(level, res_raw)
        txt_contexto = f"Se invocó a la integración {nom_int}"

        data_int.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id, 
            'MessageId': msg_id, 'Bloque': bloque_origen, 'Integracion': nom_int, 
            'Duracion_ms': extract_duration(msg), 
            'Status': status_real, 
            'Tipo_Error': tipo_error,
            'Motivo_Error': motivo_error,
            'Request_Raw': req_raw,
            'Response_Raw': res_raw,
            'Request_Legible': req_pretty,  
            'Response_Legible': res_pretty, 
            'Origen_Archivo': nombre_archivo,
            'Contexto': txt_contexto
        })

    # 3. MENSAJES Y CONTEXTO
    elif "Se estableció el valor de la variable" in msg:
        match_var = PATRON_VARIABLE.search(msg)
        if match_var:
            nombre_var = match_var.group(1).strip()
            valor_var = match_var.group(2).strip()
            txt_contexto = f"Se estableció el valor de la variable {nombre_var}"

            if nombre_var not in NOMBRES_IGNORAR:
                if nombre_var.startswith("texto") or nombre_var == "body":
                        data_msg.append({
                        'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
                        'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id, 
                        'MessageId': msg_id, 'Emisor': 'Bot', 'Texto': valor_var, 
                        'Variable_Origen': nombre_var, 'Origen_Archivo': nombre_archivo,
                        'Contexto': txt_contexto
                    })
                else:
                    data_ctx.append({
                        'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
                        'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id,
                        'MessageId': msg_id, 'Variable': nombre_var, 'Valor': valor_var, 
                        'Origen_Archivo': nombre_archivo, 'Contexto': txt_contexto
                    })
                    
    elif "hay que establecer el valor" in msg and "opcionSeleccionada" in msg:
        try: valor_boton = msg.split("establecer el valor")[1].split("a la variable")[0].strip()
        except: valor_boton = "Desconocido"
        data_msg.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id, 
            'MessageId': msg_id, 'Emisor': 'Usuario (Botón)', 'Texto': valor_boton, 
            'Variable_Origen': 'opcionSeleccionada', 'Origen_Archivo': nombre_archivo,
            'Contexto': msg
        })

    elif "textoLimpio" in msg and "Se estableció" in msg:
            try:
                valor = msg.split("expresión")[-1].strip()
                data_msg.append({
                    'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
                    'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id, 
                    'MessageId': msg_id, 'Emisor': 'Usuario', 'Texto': valor, 
                    'Variable_Origen': 'textoLimpio', 'Origen_Archivo': nombre_archivo,
                    'Contexto': 'Input de texto libre'
                })
            except: pass

    # V27: Captura de Segmentación (Mapeo a Contexto)
    elif "Segmentación de la cuenta" in msg:
        try: seg_val = msg.split("=>")[-1].strip()
        except: seg_val = "Desconocido"
        data_ctx.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id,
            'MessageId': msg_id, 'Variable': 'SegmentacionCliente', 'Valor': seg_val, 
            'Origen_Archivo': nombre_archivo, 'Contexto': msg
        })

    # 4. ERRORES DE MOTOR DE REGLAS (V27 mejorado)
    elif "produjo un error: SyntaxError" in msg:
        data_err.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id, 
            'MessageId': msg_id, 'Error_Type': 'SyntaxError (Regla Bot)', 'Detalle': msg, 
            'Origen_Archivo': nombre_archivo
        })
    elif "Hubo un error al invocar a la API de integración" in msg:
        # V26: API Crítico
        data_err.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id, 
            'MessageId': msg_id, 'Error_Type': 'Fallo Crítico API (No conectó)', 'Detalle': msg, 
            'Origen_Archivo': nombre_archivo
        })
    elif "No se continuara la ejecucion del caso porque no hay una transicion definida" in msg:
        # V26: Flujo Roto
        data_err.append({
            'Timestamp_Original_UTC': timestamp, 'Fecha_Real': fecha_real, 'Dia_Real': dia_real,
            'Intervalo_30min': intervalo, 'CaseId': case_id, 'UserId': user_id, 
            'MessageId': msg_id, 'Error_Type': 'Flujo Roto (Sin transición)', 'Detalle': 'El usuario se quedó estancado porque falta flecha de diseño', 
            'Origen_Archivo': nombre_archivo
        })

print("--- INICIANDO PROCESO V27 (Optimizada) ---")
files = glob.glob(os.path.join(INPUT_FOLDER, "*.txt"))
for f in files: process_file(f)
print("--- FIN ---")