import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
import database as db # Reciclamos tu módulo de conexión a Supabase!

# 1. Configuración de la página (Pestaña del navegador)
st.set_page_config(
    page_title="T-Envios | Panel Directivo",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# 2. Conexión a la Base de Datos
@st.cache_resource
def iniciar_db():
    try:
        db.inicializar_db()
        return True
    except Exception as e:
        st.error(f"Error conectando a la base de datos: {e}")
        return False

iniciar_db()

# 3. Encabezado Corporativo
st.title("📊 Panel Directivo - T-Envios")
st.markdown("Monitor de Rendimiento en Tiempo Real | **San Cristóbal**")
st.divider()

# 4. Filtro de Fecha
fecha_hoy = date.today()
fecha_seleccionada = st.date_input("Seleccionar Fecha de Análisis:", fecha_hoy)
fecha_str = fecha_seleccionada.strftime("%Y-%m-%d")

# 5. Obtener Datos (Usamos tus mismas funciones!)
try:
    stats = db.estadisticas_crm(fecha_str)
    visitas = db.obtener_visitas(fecha_filtro=fecha_str)
    v_dia = len(visitas) if visitas else 0
except Exception:
    stats = {"llamadas_hoy": 0, "prospectos_totales": 0, "clientes_activos": 0}
    v_dia = 0

# 6. Tarjetas de Métricas (KPIs)
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(label="📞 Llamadas (Día)", value=stats.get("llamadas_hoy", 0))
with col2:
    st.metric(label="📍 Visitas (Día)", value=v_dia)
with col3:
    st.metric(label="👥 Prospectos en BD", value=stats.get("prospectos_totales", 0))
with col4:
    st.metric(label="💼 Cartera Activa", value=stats.get("clientes_activos", 0))

st.divider()

# 7. Gráfico de Tendencia (Últimos 7 días)
st.subheader("📈 Producción últimos 7 días")

dias, calls, visits = [], [], []
for i in range(6, -1, -1):
    d = (fecha_seleccionada - timedelta(days=i)).strftime("%Y-%m-%d")
    dias.append(d[8:]) # Solo el día
    try:
        c_stats = db.estadisticas_crm(d)
        calls.append(c_stats.get("llamadas_hoy", 0))
        v_list = db.obtener_visitas(fecha_filtro=d)
        visits.append(len(v_list) if v_list else 0)
    except:
        calls.append(0)
        visits.append(0)

# Crear tabla de datos para el gráfico
df_grafico = pd.DataFrame({
    'Día': dias,
    'Llamadas': calls,
    'Visitas': visits
}).set_index('Día')

# Mostrar gráfico interactivo (Streamlit lo hace hermoso por defecto)
st.line_chart(df_grafico, color=["#a855f7", "#f59e0b"])

st.caption("Sistema de Gestión Operativa - Desarrollado para T-Envios")