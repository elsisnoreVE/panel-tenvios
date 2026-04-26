import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import re

# ─── URL DE CONEXIÓN A LA NUBE (SUPABASE) ───
DB_URL = "postgresql://postgres.hhhkqevqnmpooixjjrry:tenvios2026@aws-1-us-east-1.pooler.supabase.com:6543/postgres"

def get_connection():
    # RealDictCursor permite que los datos se lean como diccionarios, igual que en tu código original
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)

# ════════════════════════════════════════════════════════════
# INICIALIZACIÓN DE LA BASE DE DATOS EN LA NUBE
# ════════════════════════════════════════════════════════════
def inicializar_db():
    conn = get_connection()
    c = conn.cursor()

    # 1. EMPRESAS (PROSPECTOS Y CLIENTES)
    c.execute("""
        CREATE TABLE IF NOT EXISTS empresas (
            id                  SERIAL PRIMARY KEY,
            nombre_completo     TEXT NOT NULL,
            rif                 TEXT,
            web                 TEXT,
            instagram           TEXT,
            correo              TEXT,
            dueno_nombre        TEXT,
            dueno_celular       TEXT,
            encargado_nombre    TEXT,
            encargado_celular   TEXT,
            gerente_nombre      TEXT,
            gerente_celular     TEXT,
            pais                TEXT DEFAULT 'Venezuela',
            estado_region       TEXT,
            ciudad              TEXT,
            parroquia           TEXT,
            calle               TEXT,
            numero_local        TEXT,
            referencia          TEXT,
            latitud             REAL,
            longitud            REAL,
            notas               TEXT,
            estado_funnel       TEXT DEFAULT 'prospecto',
            categoria           TEXT DEFAULT 'General',
            activa              INTEGER DEFAULT 1,
            creado_en           TEXT DEFAULT to_char(timezone('America/Caracas', now()), 'YYYY-MM-DD HH24:MI:SS'),
            actualizado_en      TEXT DEFAULT to_char(timezone('America/Caracas', now()), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    # Validaciones para actualizar esquemas sin perder datos en Postgres
    c.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'empresas'")
    columnas = [row["column_name"] for row in c.fetchall()]
    if "estado_funnel" not in columnas:
        c.execute("ALTER TABLE empresas ADD COLUMN estado_funnel TEXT DEFAULT 'prospecto'")
    if "categoria" not in columnas:
        c.execute("ALTER TABLE empresas ADD COLUMN categoria TEXT DEFAULT 'General'")

    # 2. REGISTRO DE LLAMADAS (CRM)
    c.execute("""
        CREATE TABLE IF NOT EXISTS registro_llamadas (
            id SERIAL PRIMARY KEY,
            empresa_id INTEGER REFERENCES empresas(id) ON DELETE CASCADE,
            fecha TEXT NOT NULL,
            resultado TEXT,
            notas TEXT,
            creado_en TEXT DEFAULT to_char(timezone('America/Caracas', now()), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    # 3. CLIENTES (CONTACTOS MIGRADOS)
    c.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id          SERIAL PRIMARY KEY,
            nombre      TEXT NOT NULL,
            apellido    TEXT,
            celular     TEXT,
            correo      TEXT,
            cargo       TEXT,
            empresa_id  INTEGER REFERENCES empresas(id) ON DELETE CASCADE,
            notas       TEXT,
            creado_en   TEXT DEFAULT to_char(timezone('America/Caracas', now()), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    # 4. VISITAS (RUTAS)
    c.execute("""
        CREATE TABLE IF NOT EXISTS visitas (
            id              SERIAL PRIMARY KEY,
            cliente_id      INTEGER REFERENCES clientes(id) ON DELETE CASCADE,
            empresa_id      INTEGER REFERENCES empresas(id) ON DELETE CASCADE,
            fecha           TEXT NOT NULL,
            hora_programada TEXT NOT NULL,
            hora_real       TEXT,
            estado          TEXT DEFAULT 'pendiente',
            resultado       TEXT,
            notas           TEXT,
            creado_en       TEXT DEFAULT to_char(timezone('America/Caracas', now()), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    # 5. TAREAS
    c.execute("""
        CREATE TABLE IF NOT EXISTS tareas (
            id              SERIAL PRIMARY KEY,
            titulo          TEXT NOT NULL,
            descripcion     TEXT,
            prioridad       TEXT DEFAULT 'media',
            categoria       TEXT DEFAULT 'general',
            fecha_limite    TEXT,
            estado          TEXT DEFAULT 'pendiente',
            completada_en   TEXT,
            empresa_id      INTEGER REFERENCES empresas(id) ON DELETE CASCADE,
            cliente_id      INTEGER REFERENCES clientes(id) ON DELETE CASCADE,
            creado_en       TEXT DEFAULT to_char(timezone('America/Caracas', now()), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    # 6. RECORDATORIOS
    c.execute("""
        CREATE TABLE IF NOT EXISTS recordatorios (
            id          SERIAL PRIMARY KEY,
            titulo      TEXT NOT NULL,
            descripcion TEXT,
            fecha_hora  TEXT NOT NULL,
            repeticion  TEXT DEFAULT 'ninguna',
            activo      INTEGER DEFAULT 1,
            disparado   INTEGER DEFAULT 0,
            empresa_id  INTEGER REFERENCES empresas(id) ON DELETE CASCADE,
            cliente_id  INTEGER REFERENCES clientes(id) ON DELETE CASCADE,
            visita_id   INTEGER REFERENCES visitas(id) ON DELETE CASCADE,
            creado_en   TEXT DEFAULT to_char(timezone('America/Caracas', now()), 'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    conn.commit()
    conn.close()

# ════════════════════════════════════════════════════════════
# FUNCIONES DE FORMATEO Y LIMPIEZA
# ════════════════════════════════════════════════════════════
def formatear_internacional(txt):
    if not txt: return ""
    limpio = re.sub(r'\D', '', str(txt))
    if not limpio: return ""
    if limpio.startswith("0") and len(limpio) == 11: return "+58" + limpio[1:]
    elif limpio.startswith("58") and len(limpio) == 12: return "+" + limpio
    elif len(limpio) == 10: return "+58" + limpio
    else: return "+" + limpio

# ════════════════════════════════════════════════════════════
# FUNCIONES: EMPRESAS
# ════════════════════════════════════════════════════════════
def insertar_empresa(datos: dict) -> int:
    campos_por_defecto = {
        "nombre_completo": "", "rif": "", "web": "", "instagram": "", "correo": "",
        "dueno_nombre": "", "dueno_celular": "", "encargado_nombre": "", "encargado_celular": "",
        "gerente_nombre": "", "gerente_celular": "", "pais": "Venezuela", "estado_region": "Tachira",
        "ciudad": "San Cristobal", "parroquia": "", "calle": "", "numero_local": "",
        "referencia": "", "latitud": 0.0, "longitud": 0.0, "notas": "",
        "estado_funnel": "prospecto", "categoria": "General"
    }
    final_datos = {**campos_por_defecto, **datos}

    final_datos["dueno_celular"] = formatear_internacional(final_datos.get("dueno_celular"))
    final_datos["encargado_celular"] = formatear_internacional(final_datos.get("encargado_celular"))
    final_datos["gerente_celular"] = formatear_internacional(final_datos.get("gerente_celular"))

    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO empresas (
                nombre_completo, rif, web, instagram, correo,
                dueno_nombre, dueno_celular, encargado_nombre, encargado_celular,
                gerente_nombre, gerente_celular, pais, estado_region, ciudad, parroquia,
                calle, numero_local, referencia, latitud, longitud, notas, estado_funnel, categoria
            ) VALUES (
                %(nombre_completo)s, %(rif)s, %(web)s, %(instagram)s, %(correo)s,
                %(dueno_nombre)s, %(dueno_celular)s, %(encargado_nombre)s, %(encargado_celular)s,
                %(gerente_nombre)s, %(gerente_celular)s, %(pais)s, %(estado_region)s, %(ciudad)s, %(parroquia)s,
                %(calle)s, %(numero_local)s, %(referencia)s, %(latitud)s, %(longitud)s, %(notas)s, %(estado_funnel)s, %(categoria)s
            ) RETURNING id
        """, final_datos)
        rid = c.fetchone()['id']
        conn.commit()
        return rid
    except Exception as e:
        print(f"Error insertando empresa: {e}")
        return None
    finally:
        conn.close()

def obtener_empresas(solo_activas=True, solo_clientes=False):
    conn = get_connection()
    c = conn.cursor()
    try:
        q = "SELECT * FROM empresas WHERE 1=1"
        if solo_activas: q += " AND activa = 1"
        if solo_clientes: q += " AND estado_funnel = 'cliente'"
        q += " ORDER BY creado_en DESC"
        c.execute(q)
        return c.fetchall()
    finally:
        conn.close()

def obtener_empresa(empresa_id: int) -> dict:
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM empresas WHERE id = %s", (empresa_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else {}

def actualizar_empresa(empresa_id: int, datos: dict):
    datos["actualizado_en"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    conn = get_connection()
    c = conn.cursor()
    campos = ", ".join([f"{k} = %({k})s" for k in datos])
    datos["id"] = empresa_id
    c.execute(f"UPDATE empresas SET {campos} WHERE id = %(id)s", datos)
    conn.commit()
    conn.close()

def eliminar_empresa(empresa_id: int):
    conn = get_connection()
    c = conn.cursor()
    for tabla in ("visitas", "tareas", "recordatorios", "clientes", "registro_llamadas"):
        c.execute(f"DELETE FROM {tabla} WHERE empresa_id = %s", (empresa_id,))
    c.execute("DELETE FROM empresas WHERE id = %s", (empresa_id,))
    conn.commit()
    conn.close()

def buscar_empresas(termino: str) -> list:
    conn = get_connection()
    c = conn.cursor()
    t = f"%{termino}%"
    c.execute("""
        SELECT * FROM empresas
        WHERE nombre_completo ILIKE %s OR rif ILIKE %s OR ciudad ILIKE %s OR parroquia ILIKE %s OR categoria ILIKE %s
        ORDER BY nombre_completo ASC
    """, (t, t, t, t, t))
    rows = c.fetchall()
    conn.close()
    return rows

# ════════════════════════════════════════════════════════════
# FUNCIONES: CLIENTES
# ════════════════════════════════════════════════════════════
def insertar_cliente(nombre, apellido="", celular="", correo="", cargo="", empresa_id=None, notas="") -> int:
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO clientes (nombre, apellido, celular, correo, cargo, empresa_id, notas) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id", 
              (nombre, apellido, formatear_internacional(celular), correo, cargo, empresa_id, notas))
    rid = c.fetchone()['id']
    conn.commit()
    conn.close()
    return rid

def obtener_clientes(empresa_id=None):
    conn = get_connection()
    c = conn.cursor()
    if empresa_id:
        c.execute("SELECT cl.*, e.nombre_completo AS empresa_nombre FROM clientes cl LEFT JOIN empresas e ON cl.empresa_id = e.id WHERE cl.empresa_id = %s ORDER BY cl.nombre ASC", (empresa_id,))
    else:
        c.execute("SELECT cl.*, e.nombre_completo AS empresa_nombre FROM clientes cl LEFT JOIN empresas e ON cl.empresa_id = e.id ORDER BY cl.nombre ASC")
    rows = c.fetchall()
    conn.close()
    return rows

def eliminar_cliente(cliente_id: int):
    conn = get_connection()
    c = conn.cursor()
    for tabla in ("visitas", "tareas", "recordatorios"):
        c.execute(f"DELETE FROM {tabla} WHERE cliente_id = %s", (cliente_id,))
    c.execute("DELETE FROM clientes WHERE id = %s", (cliente_id,))
    conn.commit()
    conn.close()

# ════════════════════════════════════════════════════════════
# FUNCIONES: VISITAS (RUTAS LOGÍSTICAS)
# ════════════════════════════════════════════════════════════
def insertar_visita(empresa_id, fecha, hora_programada, cliente_id=None, notas="") -> int:
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO visitas (empresa_id, cliente_id, fecha, hora_programada, notas) VALUES (%s, %s, %s, %s, %s) RETURNING id", 
              (empresa_id, cliente_id, fecha, hora_programada, notas))
    rid = c.fetchone()['id']
    conn.commit()
    conn.close()
    return rid

def obtener_visitas(fecha_filtro=None, estado_filtro=None, zona_filtro=None):
    conn = get_connection()
    c = conn.cursor()
    q = """
        SELECT v.*, e.nombre_completo AS empresa_nombre, e.ciudad, e.parroquia, e.calle,
               cl.nombre || ' ' || COALESCE(cl.apellido,'') AS cliente_nombre, cl.celular AS cliente_celular 
        FROM visitas v 
        LEFT JOIN empresas e ON v.empresa_id = e.id 
        LEFT JOIN clientes cl ON v.cliente_id = cl.id 
        WHERE 1=1
    """
    params = []
    if fecha_filtro: q += " AND v.fecha = %s"; params.append(fecha_filtro)
    if estado_filtro and estado_filtro != "todos": q += " AND v.estado = %s"; params.append(estado_filtro)
    if zona_filtro:
        t = f"%{zona_filtro}%"
        q += " AND (e.parroquia ILIKE %s OR e.calle ILIKE %s OR e.ciudad ILIKE %s)"
        params.extend([t, t, t])
        
    q += " ORDER BY v.fecha ASC, v.hora_programada ASC"
    c.execute(q, params)
    rows = c.fetchall()
    conn.close()
    return rows

def marcar_visita(visita_id, estado, hora_real=None, resultado=None, notas=None):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE visitas SET estado=%s, hora_real=%s, resultado=%s, notas=COALESCE(%s,notas) WHERE id=%s", 
              (estado, hora_real, resultado, notas, visita_id))
    conn.commit()
    conn.close()

def eliminar_visita(visita_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM recordatorios WHERE visita_id=%s", (visita_id,))
    c.execute("DELETE FROM visitas WHERE id=%s", (visita_id,))
    conn.commit()
    conn.close()

def estadisticas_visitas():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as total, SUM(CASE WHEN estado='completada' THEN 1 ELSE 0 END) as completadas, SUM(CASE WHEN estado='pendiente' THEN 1 ELSE 0 END) as pendientes, SUM(CASE WHEN estado='cancelada' THEN 1 ELSE 0 END) as canceladas, SUM(CASE WHEN estado='no_asistio' THEN 1 ELSE 0 END) as no_asistio FROM visitas")
    row = c.fetchone()
    conn.close()
    return row

# ════════════════════════════════════════════════════════════
# FUNCIONES: TAREAS
# ════════════════════════════════════════════════════════════
def insertar_tarea(titulo, descripcion="", prioridad="media", categoria="general", fecha_limite=None, empresa_id=None, cliente_id=None) -> int:
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO tareas (titulo, descripcion, prioridad, categoria, fecha_limite, empresa_id, cliente_id) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id", 
              (titulo, descripcion, prioridad, categoria, fecha_limite, empresa_id, cliente_id))
    rid = c.fetchone()['id']
    conn.commit()
    conn.close()
    return rid

def obtener_tareas(estado_filtro=None, prioridad_filtro=None, categoria_filtro=None):
    conn = get_connection()
    c = conn.cursor()
    q = "SELECT t.*, e.nombre_completo AS empresa_nombre, cl.nombre || ' ' || COALESCE(cl.apellido,'') AS cliente_nombre FROM tareas t LEFT JOIN empresas e ON t.empresa_id = e.id LEFT JOIN clientes cl ON t.cliente_id = cl.id WHERE 1=1"
    params = []
    if estado_filtro and estado_filtro != "todas": q += " AND t.estado=%s"; params.append(estado_filtro)
    if prioridad_filtro and prioridad_filtro != "todas": q += " AND t.prioridad=%s"; params.append(prioridad_filtro)
    if categoria_filtro and categoria_filtro != "todas": q += " AND t.categoria=%s"; params.append(categoria_filtro)
    q += " ORDER BY CASE t.prioridad WHEN 'alta' THEN 1 WHEN 'media' THEN 2 ELSE 3 END, CASE WHEN t.fecha_limite IS NULL THEN 1 ELSE 0 END, t.fecha_limite ASC"
    c.execute(q, params)
    rows = c.fetchall()
    conn.close()
    return rows

def completar_tarea(tarea_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE tareas SET estado='completada', completada_en=to_char(timezone('America/Caracas', now()), 'YYYY-MM-DD HH24:MI:SS') WHERE id=%s", (tarea_id,))
    conn.commit()
    conn.close()

def actualizar_tarea(tarea_id, **kwargs):
    conn = get_connection()
    c = conn.cursor()
    campos = ", ".join([f"{k}=%s" for k in kwargs])
    valores = list(kwargs.values()) + [tarea_id]
    c.execute(f"UPDATE tareas SET {campos} WHERE id=%s", valores)
    conn.commit()
    conn.close()

def eliminar_tarea(tarea_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM tareas WHERE id=%s", (tarea_id,))
    conn.commit()
    conn.close()

def estadisticas_tareas():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as total, SUM(CASE WHEN estado='pendiente' THEN 1 ELSE 0 END) as pendientes, SUM(CASE WHEN estado='completada' THEN 1 ELSE 0 END) as completadas, SUM(CASE WHEN prioridad='alta' AND estado='pendiente' THEN 1 ELSE 0 END) as urgentes FROM tareas")
    row = c.fetchone()
    conn.close()
    return row

# ════════════════════════════════════════════════════════════
# FUNCIONES: RECORDATORIOS
# ════════════════════════════════════════════════════════════
def insertar_recordatorio(titulo, fecha_hora, descripcion="", repeticion="ninguna", empresa_id=None, cliente_id=None, visita_id=None) -> int:
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO recordatorios (titulo, descripcion, fecha_hora, repeticion, empresa_id, cliente_id, visita_id) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id", 
              (titulo, descripcion, fecha_hora, repeticion, empresa_id, cliente_id, visita_id))
    rid = c.fetchone()['id']
    conn.commit()
    conn.close()
    return rid

def obtener_recordatorios(solo_activos=False, solo_proximos=False):
    conn = get_connection()
    c = conn.cursor()
    q = "SELECT r.*, e.nombre_completo AS empresa_nombre, cl.nombre AS cliente_nombre FROM recordatorios r LEFT JOIN empresas e ON r.empresa_id = e.id LEFT JOIN clientes cl ON r.cliente_id = cl.id WHERE 1=1"
    if solo_activos: q += " AND r.activo=1"
    if solo_proximos: q += " AND r.fecha_hora >= to_char(timezone('America/Caracas', now()), 'YYYY-MM-DD HH24:MI:SS') AND r.activo=1"
    q += " ORDER BY r.fecha_hora ASC"
    c.execute(q)
    rows = c.fetchall()
    conn.close()
    return rows

def obtener_recordatorios_pendientes():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM recordatorios WHERE activo=1 AND disparado=0 AND fecha_hora <= to_char(timezone('America/Caracas', now()), 'YYYY-MM-DD HH24:MI:SS')")
    rows = c.fetchall()
    conn.close()
    return rows

def marcar_recordatorio_disparado(rec_id, repeticion="ninguna"):
    conn = get_connection()
    c = conn.cursor()
    if repeticion == "ninguna":
        c.execute("UPDATE recordatorios SET disparado=1, activo=0 WHERE id=%s", (rec_id,))
    else:
        c.execute("SELECT fecha_hora FROM recordatorios WHERE id=%s", (rec_id,))
        row = c.fetchone()
        if row:
            fh = datetime.strptime(row["fecha_hora"], "%Y-%m-%d %H:%M")
            delta = {"diaria": timedelta(days=1), "semanal": timedelta(weeks=1), "mensual": timedelta(days=30)}.get(repeticion, timedelta(days=1))
            c.execute("UPDATE recordatorios SET disparado=0, fecha_hora=%s WHERE id=%s", ((fh + delta).strftime("%Y-%m-%d %H:%M"), rec_id))
    conn.commit()
    conn.close()

def eliminar_recordatorio(rec_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM recordatorios WHERE id=%s", (rec_id,))
    conn.commit()
    conn.close()

def estadisticas_recordatorios():
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as total, SUM(CASE WHEN activo=1 AND disparado=0 THEN 1 ELSE 0 END) as activos, SUM(CASE WHEN fecha_hora >= to_char(timezone('America/Caracas', now()), 'YYYY-MM-DD HH24:MI:SS') AND activo=1 THEN 1 ELSE 0 END) as proximos FROM recordatorios")
    row = c.fetchone()
    conn.close()
    return row

# ════════════════════════════════════════════════════════════
# FUNCIONES: CRM (KPIS, CONVERSIÓN Y LLAMADAS)
# ════════════════════════════════════════════════════════════
def estadisticas_crm(fecha_hoy):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as c FROM empresas WHERE estado_funnel = 'prospecto'")
    prospectos = c.fetchone()['c']
    c.execute("SELECT COUNT(*) as c FROM empresas WHERE estado_funnel = 'cliente'")
    clientes = c.fetchone()['c']
    c.execute("SELECT COUNT(*) as c FROM registro_llamadas WHERE fecha = %s", (fecha_hoy,))
    llamadas_hoy = c.fetchone()['c']
    conn.close()
    return {"prospectos_totales": prospectos, "clientes_activos": clientes, "llamadas_hoy": llamadas_hoy}

def obtener_ultimas_llamadas(limite=500):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""
        SELECT l.*, e.nombre_completo as empresa_nombre 
        FROM registro_llamadas l 
        JOIN empresas e ON l.empresa_id = e.id 
        ORDER BY l.fecha DESC, l.id DESC LIMIT %s
    """, (limite,))
    rows = c.fetchall()
    conn.close()
    return rows

def registrar_llamada(empresa_id, fecha, resultado, notas):
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO registro_llamadas (empresa_id, fecha, resultado, notas) VALUES (%s, %s, %s, %s)", 
              (empresa_id, fecha, resultado, notas))
    conn.commit()
    conn.close()

def promover_a_cliente(empresa_id: int):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("UPDATE empresas SET estado_funnel = 'cliente' WHERE id = %s", (empresa_id,))
        
        c.execute("SELECT COUNT(*) as c FROM registro_llamadas WHERE empresa_id = %s", (empresa_id,))
        llamadas = c.fetchone()['c'] or 0
        
        c.execute("SELECT COUNT(*) as c FROM visitas WHERE empresa_id = %s", (empresa_id,))
        visitas = c.fetchone()['c'] or 0
        
        c.execute("SELECT * FROM empresas WHERE id = %s", (empresa_id,))
        emp = c.fetchone()
        
        contactos_a_migrar = [
            (emp.get("encargado_nombre"), emp.get("encargado_celular"), "Encargado"),
            (emp.get("dueno_nombre"), emp.get("dueno_celular"), "Dueño"),
            (emp.get("gerente_nombre"), emp.get("gerente_celular"), "Gerente")
        ]
        
        contactos_migrados = 0
        notas_kpi = f"Convertido a cliente tras {llamadas} llamadas y {visitas} visitas."
        
        for nom, cel, cargo in contactos_a_migrar:
            if nom or cel:
                c.execute("SELECT id FROM clientes WHERE empresa_id = %s AND (nombre = %s OR celular = %s)", (empresa_id, nom, cel))
                if not c.fetchone():
                    c.execute("INSERT INTO clientes (nombre, celular, cargo, empresa_id, notas) VALUES (%s, %s, %s, %s, %s)", 
                              (nom or "Contacto Principal", cel or "", cargo, empresa_id, notas_kpi))
                    contactos_migrados += 1
                    
        conn.commit()
        return llamadas, visitas, contactos_migrados
    except Exception as e:
        print(f"Error promoviendo a cliente: {e}")
        return 0, 0, 0
    finally:
        conn.close()

def obtener_estadisticas_ventas():
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT COUNT(id) as c FROM empresas WHERE estado_funnel = 'cliente'")
        total_clientes = c.fetchone()['c'] or 0

        c.execute("""
            SELECT COUNT(l.id) as c FROM registro_llamadas l 
            JOIN empresas e ON l.empresa_id = e.id 
            WHERE e.estado_funnel = 'cliente'
        """)
        total_llamadas = c.fetchone()['c'] or 0

        c.execute("""
            SELECT COUNT(v.id) as c FROM visitas v 
            JOIN empresas e ON v.empresa_id = e.id 
            WHERE e.estado_funnel = 'cliente'
        """)
        total_visitas = c.fetchone()['c'] or 0

        promedio = 0
        if total_clientes > 0:
            promedio = round((total_llamadas + total_visitas) / total_clientes, 1)

        return {"total": total_clientes, "esfuerzo_promedio": promedio}
    except Exception as e:
        print(f"Error estadísticas de ventas: {e}")
        return {"total": 0, "esfuerzo_promedio": 0}
    finally:
        conn.close()