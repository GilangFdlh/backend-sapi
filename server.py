# =============================================================================
# Impor Semua Library yang Dibutuhkan
# =============================================================================
import paho.mqtt.client as mqtt
import json, ssl, os, time, pytz
import pandas as pd
from datetime import datetime, time as dt_time
from threading import Lock, Thread
from flask import Flask, request, jsonify
import joblib
import firebase_admin
from firebase_admin import credentials, db

# =============================================================================
# KONFIGURASI APLIKASI
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Konfigurasi MQTT ---
MQTT_BROKER = "yf9ee3c8.ala.us-east-1.emqxsl.com"
MQTT_PORT = 8883
MQTT_USER = "sapi"
# DIUBAH: Ambil password dari Environment Variable
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

MQTT_TOPICS = {"wadah1": "fsh/sapi/2/water1", "wadah2": "fsh/sapi/2/water2"}

# --- Konfigurasi Firebase ---
# DIUBAH: Path tidak lagi digunakan, kita akan baca dari Environment Variable
# FIREBASE_KEY_PATH = os.path.join(BASE_DIR, 'serviceAccountKey.json')
FIREBASE_DATABASE_URL = 'https://data-konsumsi-air-default-rtdb.asia-southeast1.firebasedatabase.app/' 
MODEL_PATH = os.path.join(BASE_DIR, 'pipeline_klasifikasi_sapi.joblib')

# --- Konfigurasi Lain ---
CERTS_DIRECTORY = os.path.join(BASE_DIR, 'certs')
# ... (sisa konfigurasi sama)
# ...

# =============================================================================
# INISIALISASI (Flask, Firebase, Model)
# =============================================================================
app = Flask(__name__)

# DIUBAH: Inisialisasi Firebase dari Environment Variable
try:
    # 1. Ambil konten JSON dari environment variable
    firebase_creds_json_str = os.getenv("FIREBASE_CREDENTIALS_JSON")
    # 2. Ubah string JSON menjadi dictionary
    firebase_creds_dict = json.loads(firebase_creds_json_str)
    # 3. Buat kredensial dari dictionary
    cred = credentials.Certificate(firebase_creds_dict)
    
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred, {'databaseURL': FIREBASE_DATABASE_URL})
    print("Koneksi ke Firebase berhasil!")
except Exception as e:
    print(f"GAGAL terhubung ke Firebase, pastikan Environment Variable sudah diatur: {e}")

try:
    model_pipeline = joblib.load(MODEL_PATH)
    print(f"Pipeline model '{MODEL_PATH}' berhasil dimuat.")
except FileNotFoundError:
    print(f"ERROR: File model '{MODEL_PATH}' tidak ditemukan!")
    model_pipeline = None

int_to_label = {0: 'Sehat', 1: 'Berpotensi Sakit', 2: 'Sangat Berpotensi Sakit', 3: 'Sakit'}

# =============================================================================
# BAGIAN 1: LOGIKA PEREKAM DATA MQTT
# =============================================================================
def apply_moving_average(series, window_size):
    return series.rolling(window=window_size, min_periods=1).mean()

def calculate_daily_cumulative_consumption(df, threshold):
    if df.empty or 'filtered_volume' not in df.columns: return df
    df_copy = df.copy()
    df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'], format='mixed', dayfirst=False)
    df_copy = df_copy.set_index('timestamp')
    volume_change = df_copy['filtered_volume'].diff()
    df_copy['konsumsi_interval'] = volume_change.apply(lambda x: abs(x) if x < -threshold else 0)
    df_copy['cumulative_consumption'] = df_copy.groupby(df_copy.index.date)['konsumsi_interval'].cumsum()
    return df_copy.reset_index()

def create_ca_certificate():
    os.makedirs(CERTS_DIRECTORY, exist_ok=True)
    if not os.path.exists(CA_CERT_FILE):
        print("MQTT Listener: File sertifikat CA tidak ditemukan, membuatnya...")
        ca_cert_content = """-----BEGIN CERTIFICATE-----\nMIIDrzCCApegAwIBAgIQCDvgVpBCRrGhdWrJWZHHSjANBgkqhkiG9w0BAQUFADBh\nMQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3\nd3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBD\nQTAeFw0wNjExMTAwMDAwMDBaFw0zMTExMTAwMDAwMDBaMGExCzAJBgNVBAYTAlVT\nMRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5j\nb20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IENBMIIBIjANBgkqhkiG\nw0BAQEFAAOCAQ8AMIIBCgKCAQEA4jvhEXLeqKTTo1eqUKKPC3eQyaKl7hLOllsB\nCSDMAZOnTjC3U/dDxGkAV53ijSLdhwZAAIEJzs4bg7/fzTtxRuLWZscFs3YnFo97\nnh6Vfe63SKMI2tavegw5BmV/Sl0fvBf4q77uKNd0f3p4mVmFaG5cIzJLv07A6Fpt\n43C/dxC//AH2hdmoRBBYMql1GNXRor5H4idq9Joz+EkIYIvUX7Q6hL+hqkpMfT7P\nT19sdl6gSzeRntwi5m3OFBqOasv+zbMUZBfHWymeMr/y7vrTC0LUq7dBMtoM1O/4\ngdW7jVg/tRvoSSiicNoxBN33shbyTApOB6jtSj1etX+jkMOvJwIDAQABo2MwYTAO\nBgNVHQ8BAf8EBAMCAYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUA95QNVbR\nTLtm8KPiGxvDl7I90VUwHwYDVR0jBBgwFoAUA95QNVbRTLtm8KPiGxvDl7I90VUw\nDQYJKoZIhvcNAQEFBQADggEBAMucN6pIExIK+t1EnE9SsPTfrgT1eXkIoyQY/Esr\nhMAtudXH/vTBH1jLuG2cenTnmCmrEbXjcKChzUyImZOMkXDiqw8cvpOp/2PV5Adg\n06O/nVsJ8dWO41P0jmP6P6fbtGbfYmbW0W5BjfIttep3Sp+dWOIrWcBAI+0tKIJF\nPnlUkiaY4IBIqDfv8NZ5YBberOgOzW6sRBc4L0na4UU+Krk2U886UAb3LujEV0ls\nYSEY1QSteDwsOoBrp+uvFRTp2InBuThs4pFsiv9kuXclVzDAGySj4dzp30d8tbQk\nCAUw7C29C79Fv1C5qfPrmAESrciIxpg0X40KPMbp1ZWVbd4=\n-----END CERTIFICATE-----\n"""
        with open(CA_CERT_FILE, 'w') as f:
            f.write(ca_cert_content)

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("MQTT Listener: Berhasil terhubung ke Broker!")
        topics_to_subscribe = [(topic, 0) for topic in MQTT_TOPICS.values()]
        client.subscribe(topics_to_subscribe)
    else:
        print(f"MQTT Listener: Gagal terhubung, kode status: {rc}")

def on_message(client, userdata, message):
    try:
        wadah_id = next((id for id, topic in MQTT_TOPICS.items() if message.topic == topic), None)
        if not wadah_id: return
        
        payload = message.payload.decode("utf-8")
        data_mqtt = json.loads(payload)
        
        waktu_sekarang = datetime.now(TIMEZONE)
        timestamp_str = waktu_sekarang.strftime('%Y-%m-%d %H:%M:%S')
        date_str = waktu_sekarang.strftime('%Y-%m-%d')
        raw_volume = data_mqtt.get('volume')
        
        new_raw_data = {'timestamp': timestamp_str, 'volume': raw_volume}
        db.reference(f'data_mentah/{wadah_id}/{date_str}').push(new_raw_data)

        with data_lock:
            if all_data[wadah_id]:
                last_entry_date = pd.to_datetime(all_data[wadah_id][-1]['timestamp'], format='mixed', dayfirst=False).date()
                if waktu_sekarang.date() != last_entry_date:
                    all_data[wadah_id].clear()
            
            all_data[wadah_id].append(new_raw_data)
            df = pd.DataFrame(all_data[wadah_id])
            if df.empty: return

            df['filtered_volume'] = apply_moving_average(df['volume'], MAF_WINDOW)
            df_processed = calculate_daily_cumulative_consumption(df, KONSUMSI_THRESHOLD_ML)
            latest_data = df_processed.iloc[-1]
            latest_data_clean = {'timestamp': latest_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),'raw_volume': float(latest_data['volume']),'filtered_volume': float(latest_data['filtered_volume']),'cumulative_consumption': float(latest_data['cumulative_consumption'])}
            
            # DIUBAH: Simpan riwayat data olahan ke path harian menggunakan .push()
            db.reference(f'data_olahan/{wadah_id}/{date_str}').push(latest_data_clean)
        
        print(f"MQTT Listener: Data [{wadah_id}] diolah & diarsipkan. Kumulatif: {latest_data_clean['cumulative_consumption']:.2f} ml")

    except Exception as e:
        print(f"MQTT Listener: Error saat memproses pesan: {e}")

def start_mqtt_listener():
    create_ca_certificate()

    print("MQTT Listener: Mengambil data histori HARI INI dari Firebase...")
    today_date_str = datetime.now(TIMEZONE).strftime('%Y-%m-%d')
    for id in MQTT_TOPICS.keys():
        ref_today = db.reference(f'data_mentah/{id}/{today_date_str}')
        initial_data = ref_today.get()
        if initial_data:
            all_data[id].extend(list(initial_data.values()))

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.tls_set(ca_certs=CA_CERT_FILE, tls_version=ssl.PROTOCOL_TLS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT)
    client.loop_forever()

# =============================================================================
# BAGIAN 2: LOGIKA SERVER PREDIKSI (API)
# =============================================================================
def get_feature_jam(current_time):
    hour = current_time.hour
    if 0 <= hour < 10: return 8
    if 10 <= hour < 12: return 10
    if 12 <= hour < 14: return 12
    if 14 <= hour < 16: return 14
    if 16 <= hour <= 23: return 16
    return 8

# DIUBAH: Fungsi ini sekarang mengambil data dari riwayat data_olahan
def get_consumption_at_time(wadah_id, date_str, target_time_str):
    ref = db.reference(f'data_olahan/{wadah_id}/{date_str}')
    query_result = ref.order_by_child('timestamp').end_at(target_time_str).limit_to_last(1).get()
    if not query_result: return 0
    key = list(query_result.keys())[0]
    return query_result[key].get('cumulative_consumption', 0)

@app.route('/predict', methods=['POST'])
def predict():
    global TIMEZONE # BARIS INI DITAMBAHKAN
    
    if model_pipeline is None:
        return jsonify({'status': 'error', 'message': 'Model tidak siap, server error.'}), 500
    try:
        wadah_id = data['wadah_id']
        berat_tubuh = float(data['berat_tubuh'])
        suhu_tubuh = float(data['suhu_tubuh'])
        suhu_lingkungan = float(data['suhu_lingkungan'])
        pakan = float(data['pakan'])
    except (KeyError, TypeError, ValueError) as e:
        return jsonify({'status': 'error', 'message': f'Input tidak lengkap: {e}'}), 400

    print(f"\nAPI Server: Menerima request prediksi untuk [{wadah_id}]...")
    waktu_saat_ini = datetime.now(TIMEZONE)
    date_str = waktu_saat_ini.strftime('%Y-%m-%d')
    waktu_saat_ini_str = waktu_saat_ini.strftime('%Y-%m-%d %H:%M:%S')

    fitur_jam = get_feature_jam(waktu_saat_ini)
    fitur_konsumsi_kumulatif = get_consumption_at_time(wadah_id, date_str, waktu_saat_ini_str)

    jam_sebelumnya_map = {10: 8, 12: 10, 14: 12, 16: 14}
    kumulatif_sebelumnya = 0
    if fitur_jam > 8:
        jam_sebelumnya = jam_sebelumnya_map[fitur_jam]
        waktu_sebelumnya_str = datetime.combine(waktu_saat_ini.date(), dt_time(jam_sebelumnya, 0)).strftime('%Y-%m-%d %H:%M:%S')
        kumulatif_sebelumnya = get_consumption_at_time(wadah_id, date_str, waktu_sebelumnya_str)

    fitur_konsumsi_interval = fitur_konsumsi_kumulatif - kumulatif_sebelumnya
    fitur_gap_suhu = suhu_tubuh - suhu_lingkungan
    fitur_persentase_konsumsi = ((fitur_konsumsi_kumulatif / 1000) / berat_tubuh) * 100 if berat_tubuh > 0 else 0
    
    kolom_fitur = ['Jam', 'Konsumsi Kumulatif (ml)', 'Konsumsi Interval (ml)', 'Berat Tubuh (kg)', 'Suhu Tubuh (C°)', 'Suhu Lingkungan (C°)', 'Gap Suhu', 'Persentase Konsumsi (%)', 'Pakan (kg)']
    data_fitur = [fitur_jam, fitur_konsumsi_kumulatif, fitur_konsumsi_interval, berat_tubuh, suhu_tubuh, suhu_lingkungan, fitur_gap_suhu, fitur_persentase_konsumsi, pakan]
    input_df = pd.DataFrame([data_fitur], columns=kolom_fitur)
    
    prediksi_angka = model_pipeline.predict(input_df)[0]
    hasil_prediksi_label = int_to_label.get(prediksi_angka, "Label Tidak Dikenal")
    
    arsip_fitur_lengkap = input_df.to_dict('records')[0]
    arsip_fitur_lengkap["timestamp_prediksi"] = waktu_saat_ini_str
    arsip_fitur_lengkap["wadah_id"] = wadah_id
    arsip_fitur_lengkap["hasil_prediksi"] = hasil_prediksi_label
    db.reference(f'arsip_prediksi/{wadah_id}').push(arsip_fitur_lengkap)
    
    print(f"API Server: Hasil prediksi untuk [{wadah_id}] adalah {hasil_prediksi_label}. Arsip disimpan.")
    
    return jsonify({'status': 'success', 'hasil_prediksi': hasil_prediksi_label, 'timestamp_prediksi': waktu_saat_ini_str, 'detail_fitur': arsip_fitur_lengkap})

# =============================================================================
# MENJALANKAN KEDUA APLIKASI
# =============================================================================
if __name__ == "__main__":
    # 1. Jalankan listener MQTT di background thread
    mqtt_thread = Thread(target=start_mqtt_listener)
    mqtt_thread.daemon = True
    mqtt_thread.start()
    
    # 2. Jalankan server API Flask di main thread
    print("\nAPI Server: Memulai server Flask di http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)