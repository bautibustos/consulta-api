import httpx
import json
from datetime import datetime, timezone
import asyncio

# --- Configuración de Supabase y API ---
SUPABASE_URL_BASE = "https://zziggacoidbcxehtfcyw.supabase.co"
SUPABASE_TABLE_NAME = "matches_today"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inp6aWdnYWNvaWRiY3hlaHRmY3l3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjcwMzE3NSwiZXhwIjoyMDY4Mjc5MTc1fQ.2ApYJv4-Ir6QBCC1fLMs5c49CMSh2_tzZhcTu8yvLhw"
API_TO_CONSULT = "https://api.promiedos.com.ar/games/today"

# --- Función principal ---
async def perform_api_and_supabase_action():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Consultando la API en: {API_TO_CONSULT}")
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(API_TO_CONSULT, timeout=30.0, headers={"User-Agent": "Mozilla/5.0"})
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Status Code: {response.status_code}")
            print("Content-Type:", response.headers.get("content-type"))
            response.raise_for_status()

            if not response.text.strip():
                raise ValueError("La respuesta de la API está vacía")

            print("Longitud de la respuesta:", len(response.text))
            print("Primeros 300 caracteres:")
            print(response.text[:300])
            print("Últimos 100 caracteres:")
            print(response.text[-100:])

            parsed_data = response.json()

            # ✅ Solo guardar el contenido exacto de "leagues"
            if "leagues" not in parsed_data:
                raise KeyError("La clave 'leagues' no está presente en la respuesta de la API.")
            data_to_store = parsed_data["leagues"]

        # Preparar el payload para Supabase
        current_time_utc = datetime.now(timezone.utc).isoformat()
        payload = {
            "id": "test",
            "data": data_to_store,
            "updated_at": current_time_utc
        }

        supabase_insert_url = f"{SUPABASE_URL_BASE}/rest/v1/{SUPABASE_TABLE_NAME}"
        headers = {
            "Content-Type": "application/json",
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "Prefer": "resolution=merge-duplicates"
        }

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Enviando datos a Supabase: {supabase_insert_url}")
        async with httpx.AsyncClient() as client:
            supabase_response = await client.post(
                supabase_insert_url,
                json=payload,
                headers=headers,
                timeout=10.0
            )
            supabase_response.raise_for_status()

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Datos insertados/actualizados en Supabase correctamente.")
        print("Respuesta Supabase:", supabase_response.text)

    except httpx.RequestError as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR de red (API o Supabase): {e}")
    except httpx.HTTPStatusError as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR de status HTTP: {e}")
        print(f"Respuesta del servidor: {e.response.text}")
    except json.JSONDecodeError as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: La respuesta NO es JSON válido.")
        print("Detalle del error:", e)
        print("Contenido crudo (inicio):", response.text[:500])
    except KeyError as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Clave faltante en JSON - {e}")
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR inesperado: {e}")

# --- Bucle de ejecución ---
async def main_loop():
    print("--- Iniciando el script de consulta local ---")
    print("Presiona Ctrl+C para detener el script en cualquier momento.")
    while True:
        await perform_api_and_supabase_action()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Esperando 30 segundos para la próxima consulta...")
        await asyncio.sleep(30)

# --- Punto de entrada ---
if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nScript detenido por el usuario.")
    except Exception as e:
        print(f"Un error crítico ocurrió: {e}")
