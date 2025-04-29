# Nama file: crosscheck_maps.py
from botasaurus.browser import browser, Driver
import urllib.parse
import time # Untuk jeda sederhana jika diperlukan
import csv # Import modul csv

@browser(headless=True, reuse_driver=True, cache=True, block_images_and_css=True, parallel=5, wait_for_complete_page_load=False, run_async=True)
def check_business_on_maps(driver: Driver, search_query: str):
    """
    Memeriksa apakah LOKASI (Kabupaten/Kota) dari search_query muncul
    di dalam teks alamat (<div class="Io6YTe...">) hasil pencarian Google Maps.
    """
    # Ekstrak bagian LOKASI (Kabupaten/Kota) dari query untuk perbandingan
    location_part = None
    if " Kabupaten " in search_query:
        location_part = search_query.split(" Kabupaten ", 1)[1]
    elif " Kota " in search_query:
        location_part = search_query.split(" Kota ", 1)[1]

    if not location_part:
        # Jika tidak ada pemisah 'Kabupaten' atau 'Kota',
        # anggap seluruh query adalah lokasi? Atau kembalikan error?
        # Untuk saat ini, kita kembalikan error.
        print(f"Error: Tidak dapat mengekstrak bagian lokasi (Kabupaten/Kota) dari query: '{search_query}'")
        return {"found": False, "address_text": None, "query": search_query, "error": "Lokasi tidak terdeteksi dalam format query"}
    location_part = location_part.strip()

    # Buat URL pencarian Google Maps
    search_url = f"https://www.google.com/maps/search/{urllib.parse.quote_plus(search_query)}"
    # print(f"Mencari: '{search_query}' di URL: {search_url}") # Kurangi verbosity

    try:
        # Buka URL dan tangani cookie jika perlu
        driver.google_get(search_url, accept_google_cookies=True)

        # Selector CSS untuk div alamat
        address_selector = 'div.Io6YTe.fontBodyMedium.kR99db.fdkmkc'

        address_text = driver.get_text(address_selector)

        if address_text:
            address_text = address_text.strip()
            # print(f"Teks Alamat Ditemukan: '{address_text}'") # Kurangi verbosity
            # Bandingkan LOKASI (case-insensitive)
            if location_part.lower() in address_text.lower():
                print(f"✔️ Lokasi '{location_part}' ditemukan dalam alamat untuk query: '{search_query}'.")
                return {"found": True, "address_text": address_text, "query": search_query}
            else:
                print(f"❌ Lokasi '{location_part}' TIDAK ditemukan dalam alamat '{address_text}' untuk query: '{search_query}'.")
                return {"found": False, "address_text": address_text, "query": search_query}
        else:
            # Coba selector alternatif jika yang utama gagal?
            # Misal: selector untuk hasil pencarian pertama jika ini halaman daftar
            alt_selector_maybe = '[role="feed"] [jsaction*="info-pane.action.select-result"]' # Contoh sangat spekulatif
            try:
                first_result_text = driver.get_text(alt_selector_maybe)
                if first_result_text and location_part.lower() in first_result_text.lower():
                     print(f"⚠️ Div alamat utama tidak ditemukan, TAPI lokasi '{location_part}' ditemukan di hasil pertama untuk query: '{search_query}'. Dianggap DITEMUKAN.")
                     return {"found": True, "address_text": f"(Fallback: {first_result_text[:100]}...)", "query": search_query}
            except:
                pass # Abaikan jika selector alternatif juga gagal

            print(f"❌ Div alamat ({address_selector}) tidak ditemukan atau kosong untuk query: '{search_query}'.")
            return {"found": False, "address_text": None, "query": search_query}

    except Exception as e:
        # Tangani jika div tidak ditemukan atau error lain terjadi
        print(f"❌ Terjadi error saat mencari atau memproses div alamat untuk query '{search_query}': {e}")
        return {"found": False, "address_text": None, "query": search_query, "error": str(e)}


if __name__ == "__main__":
    # --- Konfigurasi ---
    # Variabel kabupaten_atau_kota tidak lagi digunakan untuk membangun query
    # kabupaten_atau_kota = " Kabupaten kepulauan mentawai" # Contoh, ganti sesuai kebutuhan
    nama_file_input = "bisnis.txt" # Nama file berisi daftar query lengkap
    nama_file_output_csv = "hasil_crosscheck.csv" # Nama file output CSV

    # --- Baca Daftar Query Lengkap dari File ---
    # Ganti nama variabel agar lebih jelas
    daftar_query_lengkap = []
    try:
        with open(nama_file_input, 'r', encoding='utf-8') as f:
            for line in f:
                query_dari_file = line.strip()
                if query_dari_file: # Hanya tambahkan jika baris tidak kosong
                    daftar_query_lengkap.append(query_dari_file)
        print(f"Berhasil membaca {len(daftar_query_lengkap)} query dari '{nama_file_input}'.")
    except FileNotFoundError:
        print(f"Error: File input '{nama_file_input}' tidak ditemukan.")
        print("Pastikan file tersebut ada di direktori yang sama dengan skrip dan berisi daftar query lengkap (satu per baris).")
        exit() # Keluar jika file tidak ditemukan
    except Exception as e:
        print(f"Error saat membaca file '{nama_file_input}': {e}")
        exit()

    print("-" * 30)

    hasil_semua = []
    if daftar_query_lengkap:
    # Jalankan async
        print("Menjalankan task scraping async...")
        task = check_business_on_maps(data=daftar_query_lengkap)
        print("Menunggu semua task selesai...")
        hasil_semua = task.get()  # <- INI WAJIB setelah run_async=True
        print("Proses paralel selesai.")
    else:
        print("Tidak ada query untuk dijalankan dari file.")


    # --- Ringkasan Hasil (Console) ---
    print("\n--- Ringkasan Hasil Akhir (Console) ---")
    if not hasil_semua:
        print("Tidak ada hasil untuk ditampilkan.")
    else:
        # Urutkan hasil berdasarkan query asli untuk konsistensi (opsional)
        # hasil_semua.sort(key=lambda x: x.get('query', ''))

        for hasil in hasil_semua:
            # Gunakan query asli dari hasil sebagai nama bisnis
            query_asli = hasil.get('query', 'Query tidak tersedia')
            # Status ditemukan sekarang berdasarkan cek lokasi
            status = "DITEMUKAN" if hasil.get('found') else "TIDAK DITEMUKAN"
            # Tampilkan teks alamat jika ada, atau error
            detail_info = f"(Alamat: '{hasil.get('address_text', 'N/A')}')" if hasil.get('found') or hasil.get('address_text') else ""
            error_info = f"(Error: {hasil.get('error')})" if hasil.get('error') else ""
            # Tampilkan query asli
            print(f"- {query_asli}: {status} {detail_info} {error_info}")

    # --- Simpan Hasil ke CSV ---
    print(f"\nMenyimpan hasil ke file CSV: '{nama_file_output_csv}'")
    if hasil_semua:
        try:
            with open(nama_file_output_csv, 'w', newline='', encoding='utf-8') as csvfile:
                # Kembalikan nama kolom pertama ke "Nama Usaha"
                fieldnames = ['Nama Usaha', 'Crosscheck'] # Sesuai permintaan terakhir
                writer = csv.writer(csvfile)

                writer.writerow(fieldnames) # Tulis header

                for hasil in hasil_semua:
                    # Ekstrak bagian NAMA BISNIS dari query untuk ditulis ke CSV
                    query_asli_csv = hasil.get('query', '')
                    business_name_part_csv = query_asli_csv # Default jika tidak ada pemisah
                    if " Kabupaten " in query_asli_csv:
                        business_name_part_csv = query_asli_csv.split(" Kabupaten ", 1)[0]
                    elif " Kota " in query_asli_csv:
                        business_name_part_csv = query_asli_csv.split(" Kota ", 1)[0]
                    business_name_part_csv = business_name_part_csv.strip()

                    # Jika nama bisnis kosong setelah dipisah
                    if not business_name_part_csv:
                         business_name_part_csv = query_asli_csv # Fallback ke query asli

                    # Status CSV berdasarkan hasil cek LOKASI
                    status_csv = "DITEMUKAN" if hasil.get('found') else "TIDAK DITEMUKAN"
                    # Jika ada error pada pencarian, tandai di status
                    if hasil.get('error'):
                        status_csv += " (Error)"
                    # Tulis bagian nama bisnis dan status ke CSV
                    writer.writerow([business_name_part_csv, status_csv])
            print("Hasil berhasil disimpan ke CSV.")
        except Exception as e:
            print(f"Error saat menyimpan hasil ke CSV: {e}")
    else:
        print("Tidak ada hasil untuk disimpan ke CSV.")


    print("\nCross-check Selesai.")