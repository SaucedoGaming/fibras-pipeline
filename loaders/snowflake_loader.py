import snowflake.connector
import os
from pathlib import Path

def get_connection():
    return snowflake.connector.connect(
        account   = os.environ.get("SNOWFLAKE_ACCOUNT"),
        user      = os.environ.get("SNOWFLAKE_USER"),
        password  = os.environ.get("SNOWFLAKE_PASSWORD"),
        database  = os.environ.get("SNOWFLAKE_DATABASE"),
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE"),
        schema    = "staging",
    )

def upload_to_stage(conn, local_path, stage_name="staging.fibras_landing"):
    """Sube un parquet al stage interno de Snowflake."""
    filename = Path(local_path).name
    cursor   = conn.cursor()
    result   = cursor.execute(f"PUT file://{local_path} @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    for row in result:
        print(f"  {row[0]} → {row[1]} ({row[5]})")
    cursor.close()

def copy_into(conn, stage_name, table, columns, filename):
    """Carga el parquet del stage a la tabla destino."""
    cols_sql = ",\n    ".join([f"$1:{col}::VARCHAR AS {col}" for col in columns])
    sql = f"""
        COPY INTO staging.{table}
        FROM (
            SELECT
                {cols_sql}
            FROM @{stage_name}/{filename}
        )
        FILE_FORMAT = (TYPE = PARQUET)
        FORCE = TRUE;
    """
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS staging.{table} ({', '.join([f'{c} VARCHAR' for c in columns])})")
    result = cursor.execute(sql)
    for row in result:
        print(f"  Filas cargadas: {row[0]}")
    cursor.close()


if __name__ == "__main__":
    STAGE = "staging.fibras_landing"

    TABLES = {
        "funo__emisora": (
            "extractors/output/emisora_FUNO11.parquet",
            "emisora_FUNO11.parquet",
            ["emisora", "serie", "emisora_serie", "razon_social", "isin",
             "bolsa", "tipo_valor_descripcion", "tipo_valor_id", "estatus",
             "acciones_en_circulacion", "rango_historicos", "rango_financieros",
             "dividendos_json", "_batch_uploaded_at", "_batch_blob_uri"],
        ),
        "funo__historicos": (
            "extractors/output/historicos_FUNO11.parquet",
            "historicos_FUNO11.parquet",
            ["emisora_serie", "fecha", "precio_cierre", "importe",
             "_batch_uploaded_at", "_batch_blob_uri"],
        ),
        "funo__financieros": (
            "extractors/output/financieros_FUNO11.parquet",
            "financieros_FUNO11.parquet",
            ["emisora", "periodo", "estado", "fecha_reporte", "cuenta",
             "descripcion_es", "monto", "_batch_uploaded_at", "_batch_blob_uri"],
        ),
    }

    conn = get_connection()
    print("Conexión OK")

    try:
        for table, (local_file, stage_file, columns) in TABLES.items():
            print(f"\n[{table}]")
            print(f"  Subiendo {local_file} al stage...")
            upload_to_stage(conn, str(Path(local_file).resolve()), STAGE)

            print(f"  Cargando a staging.{table}...")
            copy_into(conn, STAGE, table, columns, stage_file)
    finally:
        conn.close()
        print("\n✓ Carga completa")