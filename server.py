# =============================================================================
# Impor Semua Library yang Dibutuhkan
# =============================================================================
import paho.mqtt.client as mqtt
import json, ssl, os, time, pytz
import pandas as pd
from datetime import datetime, time as dt_time
from threading import Lock, Thread

# Library untuk Server & Model
from flask import Flask, request, jsonify
import joblib

# Library Firebase
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
MQTT_PASSWORD = "sapicjm" # Ambil dari env var, fallback ke default
MQTT_TOPICS = {"wadah1": "fsh/sapi/2/water1", "wadah2": "fsh/sapi/2/water2"}

# --- Konfigurasi Firebase ---
FIREBASE_DATABASE_URL = 'https://data-konsumsi-air-default-rtdb.asia-southeast1.firebasedatabase.app/' 
MODEL_PATH = os.path.join(BASE_DIR, 'pipeline_klasifikasi_sapi.joblib')

# --- Konfigurasi Lain ---
CERTS_DIRECTORY = os.path.join(BASE_DIR, 'certs')
CA_CERT_FILE = os.path.join(CERTS_DIRECTORY, "ca_certificate.pem")
MAF_WINDOW = 10
KONSUMSI_THRESHOLD_ML = 100
TIMEZONE = pytz.timezone('Asia/Jakarta')
data_lock = Lock()
all_data = {"wadah1": [], "wadah2": []}

# =============================================================================
# INISIALISASI (Flask, Firebase, Model)
# =============================================================================
app = Flask(__name__)

try:
    # Coba inisialisasi dari environment variable dulu (untuk Railway)
    firebase_creds_json_str = os.getenv("FIREBASE_CREDENTIALS_JSON")
    if firebase_creds_json_str:
        firebase_creds_dict = json.loads(firebase_creds_json_str)
        cred = credentials.Certificate(firebase_creds_dict)
    else:
        # Jika gagal, fallback ke file lokal (untuk testing di komputer Anda)
        FIREBASE_KEY_PATH = os.path.join(BASE_DIR, 'serviceAccountKey.json')
        cred = credentials.Certificate(FIREBASE_KEY_PATH)

    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred, {'databaseURL': FIREBASE_DATABASE_URL})
    print("Koneksi ke Firebase berhasil!")
except Exception as e:
    print(f"GAGAL terhubung ke Firebase: {e}")

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
    if pd.api.types.is_datetime64_any_dtype(df_copy.index) and df_copy.index.tz is not None:
        df_copy.index = df_copy.index.tz_localize(None)
    volume_change = df_copy['filtered_volume'].diff()
    df_copy['konsumsi_interval'] = volume_change.apply(lambda x: abs(x) if x < -threshold else 0)
    df_copy['cumulative_consumption'] = df_copy.groupby(df_copy.index.date)['konsumsi_interval'].cumsum()
    return df_copy.reset_index()

def create_ca_certificate():
    os.makedirs(CERTS_DIRECTORY, exist_ok=True)
    if not os.path.exists(CA_CERT_FILE):
        print("MQTT Listener: File sertifikat CA tidak ditemukan, membuatnya...")
        ca_cert_content = """-----BEGIN CERTIFICATE-----\nMIIDrzCCApegAwIBAgIQCDvgVpBCRrGhdWrJWZHHSjANBgkqhkiG9w0BAQUFADBh\nMQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3\nd3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBD\nQTAeFw0wNjExMTAwMDAwMDBaFw0zMTExMTAwMDAwMDBaMGExCzAJBgNVBAYTAlVT\nMRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5j\nb20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IENBMIIBIjANBgkqhkiG\nw0BAQEFAAOCAQ8AMIIBCgKCAQEA4jvhEXLeqKTTo1eqUKKPC3eQyaKl7hLOllsB\nCSDMAZOnTjC3U/dDxGkAV53ijSLdhwZAAIEJzs4bg7/fzTtxRuLWZscFs3YnFo97\nnh6Vfe63SKMI2tavegw5BmV/Sl0fvBf4q77uKNd0f3p4mVmFaG5cIzJLv07A6Fpt\n43C/dxC//AH2hdmoRBBYMql1GNXRor5H4idq9Joz+EkIYIvUX7Q6hL+hqkpMfT7P\nT19sdl6gSzeRntwi5m3OFBqOasv+zbMUZBfHWymeMr/y7vrTC0LUq7dBMtoM1O/4\ngdW7jVg/tRvoSSiicNoxBN33shbyTApOB6jtSj1etX+jkMOvJwIDAQABo2MwYTAO\nBgNVHQ8BAf8EBAMCAYYwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUA95QNVbR\nTLtm8KPiGxvDl7I90VUwHwYDVR0jBBgwFoAUA95QNVbRTLtm8KPiGxvDl7I90VUw\nDQYJKoZIhvcNAQEFBQADggEBAMucN6pIExIK+t1EnE9SsPTfrgT1eXkIoyQY/Esr\nhMAtudXH/vTBH1jLuG2cenTnmCmrEbXjcKChzUyImZOMkXDiqw8cvpOp/2PV5Adg\n06O/nVsJ8dWO41P0jmP6P6fbtGbfYmbW0W5BjfIttep3Sp+dWOIrWcBAI+0tKIJF\nPnlUkiaY4IBIqDfv8NZ5YBberOgOzW6sRBc4L0na4UU+Krk2U886UAb3LujEV0ls\nYSEY1QSteDwsOoBrp+uvFRTp2InBuThs4pFsiv9kuXclVzDAGySj4dzp30d8tbQk\nCAUw7C29C79Fv1C5qfPrmAESrciIxpg0X40KPMbp1ZWVbd4=\n-----END CERTIFICATE-----\n"""
        with open(CA_CERT_FILE, 'w') as f: f.write(ca_cert_content)

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0: print("MQTT Listener: Berhasil terhubung ke Broker!"); client.subscribe([(t, 0) for t in MQTT_TOPICS.values()])
    else: print(f"MQTT Listener: Gagal terhubung, kode status: {rc}")

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
                if waktu_sekarang.date() != last_entry_date: all_data[wadah_id].clear()
            
            all_data[wadah_id].append(new_raw_data)
            df = pd.DataFrame(all_data[wadah_id])
            if df.empty: return

            df['filtered_volume'] = apply_moving_average(df['volume'], MAF_WINDOW)
            df_processed = calculate_daily_cumulative_consumption(df, KONSUMSI_THRESHOLD_ML)
            latest_data = df_processed.iloc[-1]
            latest_data_clean = {'timestamp': latest_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S'), 'raw_volume': float(latest_data['volume']), 'filtered_volume': float(latest_data['filtered_volume']), 'cumulative_consumption': float(latest_data['cumulative_consumption'])}
            
            db.reference(f'data_olahan/{wadah_id}/{date_str}').push(latest_data_clean)
        
        print(f"MQTT Listener: Data [{wadah_id}] diolah. Kumulatif: {latest_data_clean['cumulative_consumption']:.2f} ml")
    except Exception as e:
        print(f"MQTT Listener: Error saat memproses pesan: {e}")

def start_mqtt_listener():
    """Fungsi untuk menjalankan listener MQTT di background thread."""
    # DIHAPUS: Kita tidak lagi perlu membuat file sertifikat sama sekali.
    # create_ca_certificate() 

    print("MQTT Listener: Mengambil data histori HARI INI dari Firebase...")
    today_date_str = datetime.now(TIMEZONE).strftime('%Y-%m-%d')
    for id in MQTT_TOPICS.keys():
        ref_today = db.reference(f'data_mentah/{id}/{today_date_str}')
        initial_data = ref_today.get()
        if initial_data: all_data[id].extend(list(initial_data.values()))

        ref_arsip_today = db.reference(f'arsip_harian_olahan/{id}/{today_date_str}')
        arsip_sudah_ada = ref_arsip_today.get()
        if arsip_sudah_ada:
            last_archived_hour[id] = [int(hour) for hour in arsip_sudah_ada.keys()]
            print(f"MQTT Listener: Arsip yang sudah ada untuk [{id}] hari ini: {last_archived_hour[id]}")

    # --- PERUBAHAN UTAMA ADA DI SINI ---
    # 1. Menggunakan API versi 2 untuk mengatasi DeprecationWarning
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

    # 2. Mengaktifkan TLS tanpa menunjuk ke file lokal.
    # Ini akan menggunakan daftar sertifikat terpercaya dari sistem operasi server.
    client.tls_set(tls_version=ssl.PROTOCOL_TLS)

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT)
    client.loop_forever()

# =============================================================================
# BAGIAN 2: LOGIKA SERVER PREDIKSI (API)
# =============================================================================
def get_feature_jam(current_time):
    """Menentukan nilai fitur 'Jam' berdasarkan waktu saat ini dengan aturan baru yang lebih spesifik."""
    # Ambil hanya komponen waktu (jam, menit, detik) dari datetime
    now_time = current_time.time()

    # Aturan dari rentang waktu paling akhir ke paling awal
    
    # Lebih dari atau sama dengan 14:01
    if now_time >= dt_time(14, 1):
        return 16
    # Lebih dari atau sama dengan 12:01
    elif now_time >= dt_time(12, 1):
        return 14
    # Lebih dari atau sama dengan 10:01
    elif now_time >= dt_time(10, 1):
        return 12
    # Lebih dari atau sama dengan 08:01
    elif now_time >= dt_time(8, 1):
        return 10
    # Sisanya (dari jam 00:00 hingga 08:00)
    else:
        return 8

def get_consumption_at_time(wadah_id, date_str, target_time_str):
    ref = db.reference(f'data_olahan/{wadah_id}/{date_str}')
    query_result = ref.order_by_child('timestamp').end_at(target_time_str).limit_to_last(1).get()
    if not query_result: return 0
    key = list(query_result.keys())[0]
    return query_result[key].get('cumulative_consumption', 0)

@app.route('/predict', methods=['POST'])
def predict():
    if model_pipeline is None: return jsonify({'status': 'error', 'message': 'Model tidak siap.'}), 500
    
    data = request.get_json()
    if not data: return jsonify({'status': 'error', 'message': 'Request body harus JSON.'}), 400
    
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

    # --- AWAL PERUBAHAN ---
    kumulatif_sebelumnya = 0
    if fitur_jam > 8:
        # Tentukan jam batas periode sebelumnya secara eksplisit
        if fitur_jam == 10:
            jam_batas_sebelumnya = 8
        elif fitur_jam == 12:
            jam_batas_sebelumnya = 10
        elif fitur_jam == 14:
            jam_batas_sebelumnya = 12
        elif fitur_jam == 16:
            jam_batas_sebelumnya = 14
        
        # Format query agar mencari data terakhir TEPAT pada jam batas atau sedikit sesudahnya
        # Contoh: Mencari data terakhir di rentang "08:00:00" sampai "08:00:59"
        waktu_sebelumnya_str_start = f"{date_str} {jam_batas_sebelumnya:02d}:00:00"
        
        # Menggunakan query yang sama untuk mendapatkan nilai pada waktu tersebut
        kumulatif_sebelumnya = get_consumption_at_time(wadah_id, date_str, waktu_sebelumnya_str_start)
    # --- AKHIR PERUBAHAN ---

    fitur_konsumsi_interval = fitur_konsumsi_kumulatif - kumulatif_sebelumnya
    fitur_gap_suhu = suhu_tubuh - suhu_lingkungan
    fitur_persentase_konsumsi = ((fitur_konsumsi_kumulatif / 1000) / berat_tubuh) * 100 if berat_tubuh > 0 else 0
    
    kolom_fitur = ['Jam', 'Konsumsi Kumulatif (ml)', 'Konsumsi Interval (ml)', 'Berat Tubuh (kg)', 'Suhu Tubuh (C°)', 'Suhu Lingkungan (C°)', 'Gap Suhu', 'Persentase Konsumsi (%)', 'Pakan (kg)']
    data_fitur = [fitur_jam, fitur_konsumsi_kumulatif, fitur_konsumsi_interval, berat_tubuh, suhu_tubuh, suhu_lingkungan, fitur_gap_suhu, fitur_persentase_konsumsi, pakan]
    input_df = pd.DataFrame([data_fitur], columns=kolom_fitur)
    
    prediksi_angka = model_pipeline.predict(input_df)[0]
    hasil_prediksi_label = int_to_label.get(prediksi_angka, "Label Tidak Dikenal")
    
    detail_fitur = {
        'wadah_id': wadah_id, # <-- BARIS INI DITAMBAHKAN
        'Jam': fitur_jam,
        'Konsumsi Kumulatif (ml)': round(fitur_konsumsi_kumulatif, 2),
        'Konsumsi Interval (ml)': round(fitur_konsumsi_interval, 2),
        'Berat Tubuh (kg)': berat_tubuh,
        'Suhu Tubuh (C°)': suhu_tubuh,
        'Suhu Lingkungan (C°)': suhu_lingkungan,
        'Gap Suhu': round(fitur_gap_suhu, 2),
        'Persentase Konsumsi (%)': round(fitur_persentase_konsumsi, 2),
        'Pakan (kg)': pakan
    }

    # Buat dictionary untuk arsip, tambahkan data non-fitur
    arsip_lengkap = detail_fitur.copy()
    arsip_lengkap["timestamp_prediksi"] = waktu_saat_ini_str
    arsip_lengkap["wadah_id"] = wadah_id
    arsip_lengkap["hasil_prediksi"] = hasil_prediksi_label
    
    # Simpan arsip lengkap ke Firebase
    db.reference(f'arsip_prediksi/{wadah_id}').push(arsip_lengkap)
    print(f"API Server: Hasil prediksi untuk [{wadah_id}] adalah {hasil_prediksi_label}. Arsip disimpan.")
    
    # Kirim respons kembali ke Postman/Aplikasi
    return jsonify({
        'status': 'success',
        'hasil_prediksi': hasil_prediksi_label,
        'timestamp_prediksi': waktu_saat_ini_str,
        'detail_fitur': detail_fitur # Kirim dictionary fitur yang sudah diformat
    })

# =============================================================================
# MENJALANKAN KEDUA APLIKASI
# =============================================================================
if __name__ == "__main__":
    mqtt_thread = Thread(target=start_mqtt_listener)
    mqtt_thread.daemon = True
    mqtt_thread.start()
    
    print("\nAPI Server: Memulai server Flask di http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)