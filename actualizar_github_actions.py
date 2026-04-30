"""
actualizar_github_actions.py
============================
Versión del script adaptada para correr en GitHub Actions.
- Lee el token desde variable de entorno (no desde config.txt)
- El HTML de entrada es index.html en el mismo directorio
- GitHub Actions hace el git commit/push — este script solo genera el HTML

No uses este archivo en tu Mac local — usa actualizar_dashboard.py
"""

import pandas as pd
import numpy as np
import json
import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO

# ══════════════════════════════════════════════════════
# CONFIGURACIÓN — viene de variables de entorno en GH Actions
# ══════════════════════════════════════════════════════

CSV_URL        = os.environ.get('CSV_URL', 'https://nueva.percapita.mx/api/creditos/reporte/export/csv')
DASHBOARD_HTML = Path(__file__).parent / 'index.html'
LOG_FILE       = Path(__file__).parent / 'actualizaciones.log'
UMBRAL_RUIDO   = 15
TAT_THRESHOLD  = 200

FESTIVOS = {datetime(y,m,d) for y,m,d in [
    (2025,1,1),(2025,2,3),(2025,3,17),(2025,4,17),(2025,4,18),(2025,5,1),
    (2025,9,16),(2025,11,17),(2025,12,25),
    (2026,1,1),(2026,2,2),(2026,3,16),(2026,4,2),(2026,4,3),(2026,5,1),
    (2026,9,16),(2026,11,16),(2026,12,25),
    (2027,1,1),(2027,2,1),(2027,3,15),(2027,4,1),(2027,4,2),(2027,5,1),
]}

MESES_ES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
DIAS_ES  = ['Lunes','Martes','Miércoles','Jueves','Viernes','Sábado','Domingo']

# ══════════════════════════════════════════════════════
# UTILIDADES
# ══════════════════════════════════════════════════════

def log(msg):
    ts    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"[{ts}] {msg}"
    print(linea, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(linea + "\n")
    except Exception:
        pass


def biz_hours(start, end):
    if pd.isna(start) or pd.isna(end): return np.nan
    start = pd.Timestamp(start); end = pd.Timestamp(end)
    if end <= start: return 0.0
    total = 0.0; cur = start
    while cur.date() <= end.date():
        if cur.weekday() < 5 and datetime(cur.year,cur.month,cur.day) not in FESTIVOS:
            s  = cur.replace(hour=9,  minute=0, second=0, microsecond=0)
            e  = cur.replace(hour=18, minute=0, second=0, microsecond=0)
            ds = max(cur if cur.date()==start.date() else s, s)
            de = min(end if cur.date()==end.date() else e, e)
            if de > ds: total += (de-ds).total_seconds() / 3600
        cur = (cur+timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
    return total


def tat_med(serie):
    v = serie.dropna(); v = v[v <= TAT_THRESHOLD]
    return round(float(v.median()), 2) if len(v) else 0


def bloque_horario(h):
    if 9  <= h < 11: return '09–11h'
    if 11 <= h < 13: return '11–13h'
    if 13 <= h < 15: return '13–15h'
    if 15 <= h < 17: return '15–17h'
    if 17 <= h < 18: return '17–18h'
    return 'Fuera horario'


def mes_bonito(periodo):
    t = periodo.to_timestamp()
    return f"{MESES_ES[t.month-1]} {str(t.year)[-2:]}"

# ══════════════════════════════════════════════════════
# PASO 1 — DESCARGAR CSV
# ══════════════════════════════════════════════════════

def descargar_csv(url):
    import requests
    log(f"Descargando CSV desde {url} ...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text), encoding="utf-8")
    log(f"  ✓ {len(df):,} filas descargadas")
    return df

# ══════════════════════════════════════════════════════
# PASO 2 — PREPARAR BASE (filtros metodología v8)
# ══════════════════════════════════════════════════════

def preparar_base(df):
    MAPEO = {
        'ID Crédito':'id_credito','Nombres':'nombres','Edad':'edad',
        'Fecha Solicitud':'fecha_solicitud','Motivo Rechazo':'motivo_rechazo',
        'Fecha Rechazo':'fecha_rechazo','Fecha Aceptación':'fecha_aceptacion',
        'Fecha Dictamen':'fecha_dictamen','Fecha Dispersión':'fecha_dispersion',
        'Tipo Crédito':'tipo_crediticio','Monto':'monto',
        'Monto Autorizado':'monto_autorizado','Estado Solicitud':'estado_solicitud',
        'Frecuencia Pago':'frecuencia_pago','Ciudad Título':'ciudad_titulo',
        'Sexo':'sexo','Estado Civil':'estado_civil','Título':'titulo',
        'Step':'step','Estatus':'estatus','User ID':'user_id',
    }
    df = df.rename(columns=MAPEO)
    for col in ['fecha_solicitud','fecha_dispersion','fecha_aceptacion','fecha_rechazo','fecha_dictamen']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    df = df[df['tipo_crediticio'] != 'tipo_crediticio'].copy()
    df = df[~df['tipo_crediticio'].isin(['PERSONAL','RENOVACIONPERSONAL'])].copy()
    df = df[~df['step'].isin(['solicitudPrestamoTitulo','solicitudPrestamoPersonal'])].copy()
    df = df[df['fecha_solicitud'] >= '2025-08-01'].copy()
    df = df.drop_duplicates()
    df['mes'] = df['fecha_solicitud'].dt.to_period('M')
    if 'total_intentos' not in df.columns:
        df['total_intentos'] = 1
    PASO_ORDEN = {
        'crearUsuario':1,'datosPersona':2,'telefonoPersona':3,'generaCURP':4,
        'datosDomicilio':5,'datosEmpleo':6,'datosReferenciaPersonal':7,
        'evaluacionProceso':9,'medioEntrega':10,'procesoPruebaDeVida':11,
        'loanRejected':12,'loanAceppped':13,'enEsperaDispersion':14,
        'prestamoCanceladoUsuario':14,'inicio':15,
    }
    df['paso_num'] = df['step'].map(PASO_ORDEN).fillna(0)
    dup_mask = df.duplicated(subset=['titulo','tipo_crediticio'], keep=False)
    df_unicos = df[~dup_mask].copy()
    elegidos = []
    for (t,tp), g in df[dup_mask].groupby(['titulo','tipo_crediticio']):
        comp = g[g['estatus']=='creditoAperturado']
        if   len(comp)==0: elegidos.append(g.sort_values(['paso_num','fecha_solicitud'],ascending=[False,False]).iloc[[0]])
        elif len(comp)==1: elegidos.append(comp.iloc[[0]])
        else:              elegidos.append(comp)
    if elegidos:
        df = pd.concat([df_unicos, pd.concat(elegidos,ignore_index=True)], ignore_index=True)
    log(f"  ✓ {len(df):,} registros tras filtros v8 y deduplicación")
    return df.reset_index(drop=True)

# ══════════════════════════════════════════════════════
# PASO 3 — CALCULAR TaT
# ══════════════════════════════════════════════════════

def calcular_tat(df):
    log("  Calculando TaT hábil...")
    df['tat_total'] = df.apply(lambda r: biz_hours(r['fecha_solicitud'], r['fecha_dispersion']), axis=1)
    log("  ✓ TaT calculado")
    return df

# ══════════════════════════════════════════════════════
# PASO 4 — CALCULAR INDICADORES
# ══════════════════════════════════════════════════════

def clasificar_flujo(row):
    e=row['estatus']; s=row['step']
    if e=='creditoAperturado':        return 'completado'
    if s=='prestamoCanceladoUsuario': return 'cancelado_cliente'
    if e=='errorDatosBancarios':      return 'error_datos'
    if e in ('rechazado','declinado') or s=='loanRejected': return 'rechazado'
    if s in ('enEsperaDispersion','loanAceppped','evaluacionProceso'): return 'stp_pendiente'
    return 'en_proceso'


def calcular_indicadores(df):
    df['flujo'] = df.apply(clasificar_flujo, axis=1)
    meses        = sorted(df['mes'].unique())
    mes_actual   = meses[-1]
    mes_anterior = meses[-2] if len(meses)>=2 else mes_actual
    dm  = df[df['mes']==mes_actual].copy()
    dp  = df[df['mes']==mes_anterior].copy()
    hoy = datetime.now()

    PROD_ORDER  = ['TITULO','HIBRIDO','RENOVACIONTITULO','RENOVACIONHIBRIDO']
    PROD_LABEL  = {'TITULO':'Título','HIBRIDO':'Híbrido','RENOVACIONTITULO':'Reno. Título','RENOVACIONHIBRIDO':'Reno. Híbrido'}
    PROD_COLORS = ['#3d9be8','#00c896','#ff8c42','#f5c842']
    BLOQUES     = ['09–11h','11–13h','13–15h','15–17h','17–18h','Fuera horario']
    STEP_LBL    = {
        'crearUsuario':'Crear usuario','datosPersona':'Datos personales',
        'telefonoPersona':'Teléfono','generaCURP':'Validación CURP',
        'datosDomicilio':'Datos domicilio','datosEmpleo':'Datos empleo',
        'datosReferenciaPersonal':'Referencias','evaluacionProceso':'En evaluación',
        'medioEntrega':'Medio de entrega','procesoPruebaDeVida':'Prueba de vida',
        'enEsperaDispersion':'En espera dispersión','inicio':'Dispersión iniciada',
        'loanRejected':'Rechazado sistema','prestamoCanceladoUsuario':'Cancelado cliente',
        'actualizarCuentaBancaria':'Actualizar cuenta',
    }

    def horario_mes(d):
        dh=d.copy(); dh['blq']=dh['fecha_solicitud'].dt.hour.apply(bloque_horario)
        total=len(dh)
        return [{'b':b,'n':int((dh['blq']==b).sum()),'pct':round((dh['blq']==b).sum()/total*100,1) if total else 0} for b in BLOQUES]

    tat_act  = tat_med(dm['tat_total'])
    tat_prev = tat_med(dp['tat_total'])
    tat_v  = dm['tat_total'].dropna(); tat_v  = tat_v[tat_v<=TAT_THRESHOLD]
    tat_pv = dp['tat_total'].dropna(); tat_pv = tat_pv[tat_pv<=TAT_THRESHOLD]
    md_act  = round((tat_v <=9).sum()/len(tat_v )*100,1) if len(tat_v)  else 0
    md_prev = round((tat_pv<=9).sum()/len(tat_pv)*100,1) if len(tat_pv) else 0
    meta    = round(tat_prev*0.85,2) if tat_prev>5 else round(tat_prev*0.90,2) if tat_prev>3 else round(tat_prev-0.2,2)

    productos = []
    for i,prod in enumerate(PROD_ORDER):
        g=dm[dm['tipo_crediticio']==prod]; n=len(g)
        gp=dp[dp['tipo_crediticio']==prod]
        if n==0: continue
        productos.append({'label':PROD_LABEL[prod],'n':n,'pct':round(n/len(dm)*100),
                          'tat_med':tat_med(g['tat_total']),'tat_prev':tat_med(gp['tat_total']),'color':PROD_COLORS[i]})

    pend=dm[dm['estado_solicitud']=='PENDIENTE'].copy()
    pend['dias']=(hoy-pend['fecha_solicitud']).dt.days.clip(lower=0)
    atacables=pend[pend['dias']<=UMBRAL_RUIDO]; ruido=pend[pend['dias']>UMBRAL_RUIDO]

    clientes=[]
    for _,r in atacables.sort_values('dias',ascending=False).iterrows():
        d=int(r['dias'])
        clientes.append({
            'titulo':   str(r['titulo']).strip() if pd.notna(r['titulo']) else '—',
            'producto': PROD_LABEL.get(str(r['tipo_crediticio']),'—'),
            'paso':     STEP_LBL.get(str(r['step']),str(r['step'])),
            'paso_raw': str(r['step']),
            'dias':     d,
            'urgencia': '🔴' if d>=10 else ('🟡' if d>=7 else '🟢'),
            'fecha':    r['fecha_solicitud'].strftime('%d/%m') if pd.notna(r['fecha_solicitud']) else '—',
            'ciudad':   str(r['ciudad_titulo']) if pd.notna(r.get('ciudad_titulo')) else '—',
        })

    D = {
        'mes_label':          mes_bonito(mes_actual).upper(),
        'mes_anterior_label': mes_bonito(mes_anterior),
        'fecha_corte':        f"{DIAS_ES[hoy.weekday()]} {hoy.day} {MESES_ES[hoy.month-1]} {hoy.year}",
        'hora_actualizacion': hoy.strftime('%H:%M'),
        'n':                  len(dm),
        'tat_med':            tat_act,
        'tat_anterior':       tat_prev,
        'meta':               meta,
        'monto':              round(float(dm['monto_autorizado'].sum())),
        'mismodia_pct':       md_act,
        'mismodia_prev':      md_prev,
        'cola_marzo_total':   len(pend),
        'cola_atacable':      len(atacables),
        'cola_ruido':         len(ruido),
        'horario_mar':        horario_mes(dm),
        'horario_feb':        horario_mes(dp),
        'productos':          productos,
        'clientes_atacables': clientes,
    }
    log(f"  ✓ {mes_bonito(mes_actual)} | TaT {tat_act}h | Meta {meta}h | {len(dm)} sol | {len(atacables)} atacables")
    return D

# ══════════════════════════════════════════════════════
# PASO 5 — INYECTAR DATOS EN index.html
# ══════════════════════════════════════════════════════

def inyectar_en_html(D, ruta_html):
    if not ruta_html.exists():
        raise FileNotFoundError(f"No encontré: {ruta_html}")
    with open(ruta_html,'r',encoding='utf-8') as f:
        html = f.read()

    START='const D = {'; END='\n};\n'
    ini=html.find(START)
    if ini==-1: raise ValueError("No encontré 'const D = {' en el HTML")
    fin=html.find(END,ini)
    if fin==-1: raise ValueError("No encontré el cierre '};' del bloque de datos")
    fin+=len(END)

    nuevo_bloque = f"""const D = {{
  // Actualizado automáticamente — {D['fecha_corte']} {D['hora_actualizacion']}
  mes_label: '{D['mes_label']}',
  mes_anterior_label: '{D['mes_anterior_label']}',
  fecha_corte: '{D['fecha_corte']}',
  hora_actualizacion: '{D['hora_actualizacion']}',
  n: {D['n']},
  tat_med: {D['tat_med']},
  tat_anterior: {D['tat_anterior']},
  meta: {D['meta']},
  monto: {D['monto']},
  mismodia_pct: {D['mismodia_pct']},
  mismodia_prev: {D['mismodia_prev']},
  cola_marzo_total: {D['cola_marzo_total']},
  cola_atacable: {D['cola_atacable']},
  cola_ruido: {D['cola_ruido']},
  horario_mar: {json.dumps(D['horario_mar'], ensure_ascii=False)},
  horario_feb: {json.dumps(D['horario_feb'], ensure_ascii=False)},
  productos: {json.dumps(D['productos'], ensure_ascii=False, indent=4)},
  clientes_atacables: {json.dumps(D['clientes_atacables'], ensure_ascii=False, indent=2)},
}};
"""
    html_nuevo = html[:ini] + nuevo_bloque + html[fin:]
    with open(ruta_html,'w',encoding='utf-8') as f:
        f.write(html_nuevo)
    log(f"  ✓ index.html actualizado")
    return html_nuevo

# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════

def main():
    log("="*55)
    log("INICIO — ACTUALIZACIÓN DEL DASHBOARD (GitHub Actions)")
    log("="*55)
    try:
        import requests
        df = descargar_csv(CSV_URL)
        df = preparar_base(df)
        df = calcular_tat(df)
        D  = calcular_indicadores(df)
        inyectar_en_html(D, DASHBOARD_HTML)
        log("✅ ACTUALIZACIÓN COMPLETADA")
        log("="*55)
    except Exception as e:
        log(f"❌ ERROR: {e}")
        log(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
