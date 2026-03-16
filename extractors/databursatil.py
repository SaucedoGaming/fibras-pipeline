import requests
import json
import os
from datetime import date

TOKEN = os.environ.get("DATABURSATIL_TOKEN")
BASE_URL = "https://api.databursatil.com/v2"

def _get(endpoint, params):
    """Hace el request y maneja errores básicos."""
    params["token"] = TOKEN
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def get_emisora(emisora, serie):
    """Info general + dividendos. Ej: emisora='FUNO', serie='11'"""
    data = _get("emisoras", {"letra": emisora, "mercado": "local"})
    return data.get(emisora, {}).get(serie, {})

def get_historicos(emisora_serie, inicio, fin):
    """
    Precios históricos al cierre.
    emisora_serie: 'FUNO11'
    inicio / fin: 'YYYY-MM-DD'
    """
    data = _get("historicos", {
        "emisora_serie": emisora_serie,
        "inicio": inicio,
        "final": fin,
    })
    # Normalizar: {"2025-01-02": [precio, importe], ...}
    # → [{"fecha": "2025-01-02", "precio": 21.11, "importe": 37879606.0}, ...]
    rows = []
    for fecha, valores in data.items():
        rows.append({
            "emisora_serie": emisora_serie,
            "fecha":         fecha,
            "precio_cierre": valores[0],
            "importe":       valores[1],
        })
    return rows

def get_financieros(emisora, periodo):
    """
    Estados financieros de un trimestre.
    emisora: 'FUNO' (sin serie)
    periodo: '3T_2024'
    """
    data = _get("financieros", {
        "emisora":     emisora,
        "periodo":     periodo,
        "financieros": "posicion,flujos,resultado_trimestre,resultado_acumulado",
    })
    return data

def save_json(data, filename):
    """Guarda en un archivo JSON local (por ahora, antes de agregar MinIO)."""
    os.makedirs("output", exist_ok=True)
    path = f"output/{filename}"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Guardado: {path}")
    
def get_todos_los_financieros(emisora, serie):
    """
    Descarga todos los trimestres disponibles para una emisora.
    Lee el rango_financieros directo del endpoint de emisoras.
    """
    info = get_emisora(emisora, serie)
    periodos_raw = info.get("rango_financieros", "")
    
    if not periodos_raw:
        print(f"No se encontraron periodos para {emisora}{serie}")
        return {}

    periodos = [p.strip() for p in periodos_raw.split(",")]
    print(f"  Periodos encontrados: {len(periodos)} trimestres ({periodos[0]} → {periodos[-1]})")

    todos = {}
    for periodo in periodos:
        print(f"  Descargando {periodo}...", end=" ")
        try:
            data = get_financieros(emisora, periodo)
            todos[periodo] = data
            print("OK")
        except Exception as e:
            print(f"ERROR: {e}")

    return todos


if __name__ == "__main__":
    EMISORA       = "FUNO"
    SERIE         = "11"
    EMISORA_SERIE = f"{EMISORA}{SERIE}"

    print("=== 1. Emisora ===")
    emisora = get_emisora(EMISORA, SERIE)
    save_json(emisora, f"emisora_{EMISORA_SERIE}.json")

    print("\n=== 2. Historicos (todo el historial) ===")
    historicos = get_historicos(EMISORA_SERIE, "2011-01-01", str(date.today()))
    save_json(historicos, f"historicos_{EMISORA_SERIE}.json")
    print(f"  {len(historicos)} registros descargados")

    print("\n=== 3. Financieros (todos los trimestres) ===")
    financieros = get_todos_los_financieros(EMISORA, SERIE)
    save_json(financieros, f"financieros_{EMISORA_SERIE}.json")
    print(f"  {len(financieros)} trimestres guardados")