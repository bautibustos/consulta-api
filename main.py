from dotenv import load_dotenv
import os
import httpx
import json
from datetime import datetime, timezone
import asyncio
import pytz # Importa pytz para manejo de zonas horarias

# Carga las variables de entorno del archivo .env
# ¡Esta línea debe ser una de las primeras en ejecutarse!
load_dotenv() 

# --- Configuración de Supabase y API ---
# os.getenv() sin valores por defecto. Si la variable de entorno no existe, retornará None.
SUPABASE_URL_BASE = os.getenv("SUPABASE_URL_BASE")
SUPABASE_TABLE_NAME = os.getenv("SUPABASE_TABLE_NAME")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
API_TO_CONSULT = os.getenv("API_TO_CONSULT")

# --- Validación de variables de entorno ---
# Es crucial validar que las variables no sean None antes de usarlas
if not all([SUPABASE_URL_BASE, SUPABASE_TABLE_NAME, SUPABASE_SERVICE_ROLE_KEY, API_TO_CONSULT]):
    missing_vars = [
        name for name, value in {
            "SUPABASE_URL_BASE": SUPABASE_URL_BASE,
            "SUPABASE_TABLE_NAME": SUPABASE_TABLE_NAME,
            "SUPABASE_SERVICE_ROLE_KEY": SUPABASE_SERVICE_ROLE_KEY,
            "API_TO_CONSULT": API_TO_CONSULT
        }.items() if value is None
    ]
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Las siguientes variables de entorno no están configuradas: {', '.join(missing_vars)}")
    exit(1) # Termina el script si faltan variables

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

            # Solo guardar el contenido exacto de "leagues"
            if "leagues" not in parsed_data:
                raise KeyError("La clave 'leagues' no está presente en la respuesta de la API.")
            data_to_store = parsed_data["leagues"]

            # --- Lógica de CONVERSIÓN DE ZONA HORARIA ---
            # Asume que la API de Promiedos devuelve horarios en la zona horaria de Buenos Aires (UTC-3).
            promiedos_timezone = pytz.timezone('America/Argentina/Buenos_Aires') # Zona horaria de la API
            target_timezone = pytz.timezone('America/Argentina/Cordoba') # Tu zona horaria deseada (Córdoba, Argentina)

            # Itera sobre tus datos para encontrar y convertir los horarios
            # Esta iteración asume que 'data_to_store' es una lista de ligas, y cada liga
            # tiene una clave 'games' que es una lista de partidos, y cada partido tiene 'start_time'.
            # AJUSTA ESTAS ESTRUCTURAS DE DATOS SI TU JSON ES DIFERENTE.
            for league in data_to_store:
                if 'games' in league and isinstance(league['games'], list):
                    for game in league['games']:
                        if 'start_time' in game and isinstance(game['start_time'], str):
                            try:
                                time_str = game['start_time']
                                
                                # Parsear la fecha y hora completa con el formato "DD-MM-YYYY HH:MM"
                                original_dt_naive = datetime.strptime(time_str, "%d-%m-%Y %H:%M")
                                
                                # Asignar la zona horaria original (Promiedos) al objeto datetime "naive"
                                original_dt_aware = promiedos_timezone.localize(original_dt_naive)
                                
                                # Convierte a la zona horaria objetivo
                                converted_dt = original_dt_aware.astimezone(target_timezone)
                                
                                # Actualiza el campo de tiempo en tu estructura de datos
                                # Puedes guardar el original y el convertido en diferentes campos si lo necesitas
                                game['start_time_original'] = time_str
                                game['start_time'] = converted_dt.strftime("%d-%m-%Y %H:%M") # Formato de salida deseado
                                game['start_time_iso_target_tz'] = converted_dt.isoformat() # Opcional: para guardar con info de zona horaria
                                
                            except ValueError as ve:
                                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Advertencia: No se pudo parsear el tiempo '{game.get('start_time')}' - {ve}")
                            except Exception as ex:
                                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Advertencia: Error inesperado al convertir tiempo '{game.get('start_time')}' - {ex}")
            # --- FIN de Lógica de CONVERSIÓN DE ZONA HORARIA ---

        # Preparar el payload para Supabase
        current_time_utc = datetime.now(timezone.utc).isoformat()
        payload = {
            "id": "test", # Asumiendo que 'id' sigue siendo "test" como en tu script original
            "data": data_to_store, # 'data_to_store' ahora contiene los horarios posiblemente convertidos
            "updated_at": current_time_utc
        }

        supabase_insert_url = f"{SUPABASE_URL_BASE}/rest/v1/{SUPABASE_TABLE_NAME}"
        headers = {
            "Content-Type": "application/json",
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            "Prefer": "resolution=merge-duplicates" # Para insertar o actualizar si ya existe el 'id'
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
        # Se verifica si 'response' existe antes de intentar acceder a 'response.text'
        print("Contenido crudo (inicio):", response.text[:500] if 'response' in locals() else "No response content available")
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