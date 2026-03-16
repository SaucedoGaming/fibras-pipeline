import requests
import os
import pandas as pd
from datetime import datetime, date

TOKEN    = os.environ.get("DATABURSATIL_TOKEN")
BASE_URL = "https://api.databursatil.com/v2"
OUTPUT   = "output"


# ─── HTTP ──────────────────────────────────────────────────────────────────────

def _get(endpoint, params):
    params["token"] = TOKEN
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
    response.raise_for_status()
    return response.json()


# ─── Extracción ────────────────────────────────────────────────────────────────

def get_emisora(emisora, serie):
    data = _get("emisoras", {"letra": emisora, "mercado": "local"})
    return data.get(emisora, {}).get(serie, {})

def get_historicos(emisora_serie, inicio, fin):
    return _get("historicos", {
        "emisora_serie": emisora_serie,
        "inicio":        inicio,
        "final":         fin,
    })

def get_financieros(emisora, periodo):
    return _get("financieros", {
        "emisora":     emisora,
        "periodo":     periodo,
        "financieros": "posicion,flujos,resultado_trimestre,resultado_acumulado",
    })

def get_todos_los_financieros(emisora, serie):
    info     = get_emisora(emisora, serie)
    periodos = [p.strip() for p in info.get("rango_financieros", "").split(",")]
    print(f"  {len(periodos)} trimestres encontrados ({periodos[0]} → {periodos[-1]})")

    todos = {}
    for periodo in periodos:
        print(f"  Descargando {periodo}...", end=" ", flush=True)
        try:
            todos[periodo] = get_financieros(emisora, periodo)
            print("OK")
        except Exception as e:
            print(f"ERROR: {e}")
    return todos


# ─── Transformación a DataFrames planos ────────────────────────────────────────

def transform_emisora(raw, emisora, serie, blob_uri):
    """1 fila con info general. Dividendos como JSON string (VARIANT en Snowflake)."""
    import json
    batch_at = datetime.utcnow().isoformat()
    return pd.DataFrame([{
        "emisora":                 emisora,
        "serie":                   serie,
        "emisora_serie":           f"{emisora}{serie}",
        "razon_social":            raw.get("razon_social"),
        "isin":                    raw.get("isin"),
        "bolsa":                   raw.get("bolsa"),
        "tipo_valor_descripcion":  raw.get("tipo_valor_descripcion"),
        "tipo_valor_id":           raw.get("tipo_valor_id"),
        "estatus":                 raw.get("estatus"),
        "acciones_en_circulacion": raw.get("acciones_en_circulacion"),
        "rango_historicos":        raw.get("rango_historicos"),
        "rango_financieros":       raw.get("rango_financieros"),
        "dividendos_json":         json.dumps(raw.get("dividendos", {}), ensure_ascii=False),
        "_batch_uploaded_at":      batch_at,
        "_batch_blob_uri":         blob_uri,
    }])


def transform_historicos(raw, emisora_serie, blob_uri):
    """1 fila por día de precio."""
    batch_at = datetime.utcnow().isoformat()
    rows = []
    for fecha, valores in raw.items():
        rows.append({
            "emisora_serie":      emisora_serie,
            "fecha":              fecha,
            "precio_cierre":      valores[0],
            "importe":            valores[1],
            "_batch_uploaded_at": batch_at,
            "_batch_blob_uri":    blob_uri,
        })
    df = pd.DataFrame(rows)
    df["fecha"] = pd.to_datetime(df["fecha"])
    return df


def transform_financieros(todos_periodos, emisora, blob_uri):
    """
    1 fila por (periodo, estado, fecha_reporte, cuenta).
    Estructura plana lista para Snowflake — la selección de cuentas
    relevantes se hace en dbt.
    """
    batch_at = datetime.utcnow().isoformat()
    rows = []

    for periodo, estados in todos_periodos.items():
        for estado, fechas in estados.items():
            for fecha_reporte, cuentas in fechas.items():
                for cuenta, valor in cuentas.items():
                    if isinstance(valor, list) and len(valor) == 2:
                        descripcion_es = valor[0]
                        monto          = valor[1] if not isinstance(valor[1], str) else None
                    else:
                        descripcion_es = str(valor)
                        monto          = None

                    rows.append({
                        "emisora":            emisora,
                        "periodo":            periodo,
                        "estado":             estado,
                        "fecha_reporte":      fecha_reporte,
                        "cuenta":             cuenta,
                        "descripcion_es":     descripcion_es,
                        "monto":              monto,
                        "_batch_uploaded_at": batch_at,
                        "_batch_blob_uri":    blob_uri,
                    })

    df = pd.DataFrame(rows)
    df = df[df["monto"].notna()]
    return df


# ─── Persistencia ──────────────────────────────────────────────────────────────

def save_parquet(df, filename):
    os.makedirs(OUTPUT, exist_ok=True)
    path = f"{OUTPUT}/{filename}"
    df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
    print(f"  Guardado: {path} ({len(df)} filas, {os.path.getsize(path) / 1024:.1f} KB)")
    return path


# ─── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    EMISORA       = "FUNO"
    SERIE         = "11"
    EMISORA_SERIE = f"{EMISORA}{SERIE}"

    print(f"\n{'='*50}")
    print(f" Extrayendo: {EMISORA_SERIE}")
    print(f"{'='*50}")

    # 1. Emisora
    print("\n[1/3] Emisora...")
    raw_emisora  = get_emisora(EMISORA, SERIE)
    blob_emisora = f"output/emisora_{EMISORA_SERIE}.parquet"
    df_emisora   = transform_emisora(raw_emisora, EMISORA, SERIE, blob_emisora)
    save_parquet(df_emisora, f"emisora_{EMISORA_SERIE}.parquet")

    # 2. Históricos
    print("\n[2/3] Históricos...")
    rango     = raw_emisora.get("rango_historicos", "")
    inicio    = rango.split(" a ")[0].strip() if " a " in rango else "2011-01-01"
    raw_hist  = get_historicos(EMISORA_SERIE, inicio, str(date.today()))
    blob_hist = f"output/historicos_{EMISORA_SERIE}.parquet"
    df_hist   = transform_historicos(raw_hist, EMISORA_SERIE, blob_hist)
    save_parquet(df_hist, f"historicos_{EMISORA_SERIE}.parquet")

    # 3. Financieros
    print("\n[3/3] Financieros...")
    raw_fin   = get_todos_los_financieros(EMISORA, SERIE)
    blob_fin  = f"output/financieros_{EMISORA_SERIE}.parquet"
    df_fin    = transform_financieros(raw_fin, EMISORA, blob_fin)
    save_parquet(df_fin, f"financieros_{EMISORA_SERIE}.parquet")

    print(f"\n✓ Extracción completa para {EMISORA_SERIE}")