from botasaurus.browser import browser, Driver, AsyncQueueResult
from botasaurus.request import request, Request
from botasaurus.lang import Lang
import json
import re
from fuzzywuzzy import fuzz
import csv

def extract_list_data(html):
    try:
        # Indeks baru untuk nama dan alamat
        data_string = json.loads(
            html.split(";window.APP_INITIALIZATION_STATE=")[1].split(";window.APP_FLAGS")[0]
        )[9][0] 

        if not data_string or '·' not in data_string:
            return None, None

        # Pisahkan nama dan alamat
        parts = data_string.split('·', 1)
        compared_name = parts[0].strip()
        address_part = parts[1].strip() if len(parts) > 1 else ""

        # Ekstrak Kabupaten/Kota dari alamat
        location_match = re.search(r'(Kabupaten|Kota)\s+([^,]+)', address_part, re.IGNORECASE)
        compared_location = ""
        if location_match:
            # Ambil "Kabupaten/Kota NamaLokasi"
            compared_location = f"{location_match.group(1).strip()} {location_match.group(2).strip()}"
        
        return compared_name, compared_location

    except Exception as e:
        # print(f"Debug extract_list_data error: {e}") # Opsional: uncomment untuk debug
        return None, None

# Fungsi untuk melakukan validasi kesamaan nama usaha
def validation(business_name, compared_name, business_location, compared_location):
    if not business_name or not compared_name:
        # Validasi nama gagal jika salah satu kosong
        return False

    # --- Validasi Nama (Tahap 1) ---
    # Cek fuzzy ratio
    ratio = fuzz.ratio(business_name.lower(), compared_name.lower())
    partial_ratio = fuzz.partial_ratio(business_name.lower(), compared_name.lower())
    
    name_match = (ratio >= 75 or partial_ratio >= 90)

    if not name_match:
        # Jika nama tidak cocok, langsung return False
        return False

    # --- Validasi Lokasi (Tahap 2 - Hanya jika nama cocok) ---
    if not business_location or not compared_location:
        # Jika salah satu data lokasi tidak ada, anggap lokasi tidak cocok (atau bisa diubah jadi True jika hanya nama yg penting)
        print(f"Peringatan: Data lokasi tidak lengkap untuk validasi ({business_name} vs {compared_name}). BusinessLoc: '{business_location}', ComparedLoc: '{compared_location}'")
        return False # Atau True jika lokasi tidak wajib

    # Hapus "Kabupaten"/"Kota" untuk perbandingan fuzzy lokasi yang lebih baik
    clean_business_loc = re.sub(r'^(Kabupaten|Kota)\s+', '', business_location, flags=re.IGNORECASE).strip()
    clean_compared_loc = re.sub(r'^(Kabupaten|Kota)\s+', '', compared_location, flags=re.IGNORECASE).strip()

    if not clean_business_loc or not clean_compared_loc:
         print(f"Peringatan: Data lokasi bersih tidak lengkap untuk validasi ({business_name} vs {compared_name}). CleanBusinessLoc: '{clean_business_loc}', CleanComparedLoc: '{clean_compared_loc}'")
         return False # Lokasi tidak bisa dibandingkan

    # Cek fuzzy ratio lokasi
    loc_ratio = fuzz.ratio(clean_business_loc.lower(), clean_compared_loc.lower())
    loc_partial_ratio = fuzz.partial_ratio(clean_business_loc.lower(), clean_compared_loc.lower())

    location_match = (loc_ratio >= 75 or loc_partial_ratio >= 90)

    # Hanya return True jika NAMA dan LOKASI cocok
    return location_match

# Ekstrak nama usaha dari query
def extract_business_name(query):
    # Ekstrak nama dan lokasi lengkap ("Kabupaten X" / "Kota Y")
    match_kab = re.search(r'(.+?)\s+(Kabupaten\s+.+)', query, re.IGNORECASE)
    match_kota = re.search(r'(.+?)\s+(Kota\s+.+)', query, re.IGNORECASE)
    
    if match_kab:
        business_name = match_kab.group(1).strip()
        location = match_kab.group(2).strip()
        return business_name, location
    elif match_kota:
        business_name = match_kota.group(1).strip()
        location = match_kota.group(2).strip()
        return business_name, location
    else:
        # Jika tidak ada pemisah Kabupaten/Kota, anggap seluruh query adalah nama
        # dan lokasi kosong (atau bisa disesuaikan jika ada pola lain)
        return query, ""

@request(parallel=5, async_queue=True, max_retry=5, output=None)
def scrape_place_title(request: Request, link, metadata):
    business_name = metadata["business_name"]
    business_location = metadata["business_location"] # Ambil lokasi dari metadata
    cookies = metadata["cookies"]
    
    try:
        html = request.get(link, cookies=cookies, timeout=12).text
        # Gunakan fungsi ekstraksi data yang baru
        compared_name, compared_location = extract_list_data(html) 
        
        if compared_name: # Cukup cek nama, validasi lengkap di fungsi validation
            # Lakukan validasi lengkap (nama & lokasi)
            is_found = validation(business_name, compared_name, business_location, compared_location) 
            return (link, is_found) # Hanya kembalikan link dan status found
        return (link, False) # Jika nama tidak bisa diekstrak, anggap tidak ditemukan
    except Exception as e:
        print(f"Error scraping {link}: {e}")
        return (link, False)

@browser(block_images_and_css=True,
         output=None,
         reuse_driver=True,
         wait_for_complete_page_load=False,
         lang=Lang.Indonesian,
         cache=True)
def crosscheck_business(driver: Driver, query):
    # Dapatkan nama dan lokasi dari query awal
    business_name, business_location = extract_business_name(query) 
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    
    try:
        driver.google_get(search_url, accept_google_cookies=True)
        
        compared_name = ""
        compared_location = ""
        is_list_view = False

        # Coba ambil H1 dulu untuk deteksi jenis halaman
        try:
            h1_text = driver.get_text("h1")
            if h1_text and ("hasil" in h1_text.lower() or "results" in h1_text.lower()):
                is_list_view = True
            else:
                 compared_name = h1_text # Jika bukan list view, H1 adalah nama pembanding
        except Exception:
            # Jika H1 tidak ada, mungkin ini list view atau halaman error
             is_list_view = True # Asumsikan list view jika H1 error

        # --- Proses Halaman List View ---
        if is_list_view:
            links = driver.get_all_links('[role="feed"] > div > div > a')
            links = links[:5]
            
            if not links:
                return (business_name, query, False)
            
            scrape_place_obj: AsyncQueueResult = scrape_place_title()
            cookies = driver.get_cookies_dict()
            
            # Tambahkan lokasi ke metadata
            scrape_place_obj.put(links, metadata={"business_name": business_name, 
                                                  "business_location": business_location, 
                                                  "cookies": cookies})
            
            results = scrape_place_obj.get()

            # Proses hasil list view yang sekarang (link, is_found)
            # Botasaurus mungkin masih mengembalikan flat list
            final_found_status = False
            if isinstance(results, list) and len(results) > 0:
                 if isinstance(results[0], tuple): # Jika hasilnya list of tuples (link, is_found)
                      for _link, is_found in results:
                           if is_found:
                                final_found_status = True
                                break
                 else: # Jika hasilnya flat list [link1, found1, link2, found2, ...]
                      for i in range(0, len(results), 2):
                           if i+1 < len(results):
                                is_found = results[i+1]
                                if is_found:
                                     final_found_status = True
                                     break

            return (business_name, query, final_found_status)

        # --- Proses Halaman Profil ---
        else:
            # Ambil lokasi pembanding dari div
            try:
                # Selector CSS untuk div alamat
                location_selector = "div.Io6YTe.fontBodyMedium.kR99db.fdkmkc" 
                address_text = driver.get_text(location_selector)
                
                # Ekstrak Kabupaten/Kota dari alamat
                location_match = re.search(r'(Kabupaten|Kota)\s+([^,]+)', address_text, re.IGNORECASE)
                if location_match:
                    compared_location = f"{location_match.group(1).strip()} {location_match.group(2).strip()}"
            except Exception:
                 # Jika div lokasi tidak ditemukan, biarkan compared_location kosong
                 print(f"Peringatan: Div lokasi tidak ditemukan untuk {query}")
                 compared_location = "" 

            # Lakukan validasi lengkap (nama & lokasi)
            is_found = validation(business_name, compared_name, business_location, compared_location)
            return (business_name, query, is_found)
            
    except Exception as e:
        print(f"Error processing {query}: {e}")
        return (business_name, query, False)

def load_businesses_from_file(file_path):
    businesses = []
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if line:
                    # Membersihkan karakter khusus <>()
                    line = re.sub(r'[<>()]', '', line)
                    
                    # Memperbaiki format badan hukum (PT/CV) yang di akhir nama
                    # Pola: "NAMA, (kata) LOKASI" -> "(kata) NAMA LOKASI"
                    matches = re.match(r'(.+?),\s*([^,]+?)\s+(Kabupaten|Kota)\s+(.+)', line, re.IGNORECASE)
                    if matches:
                        nama = matches.group(1).strip()
                        kata = matches.group(2).strip().upper()
                        tipe_lokasi = matches.group(3).strip()
                        lokasi = matches.group(4).strip()
                        line = f"{kata} {nama} {tipe_lokasi} {lokasi}"
                    
                    businesses.append(line)
        return businesses
    except Exception as e:
        print(f"Error loading businesses: {e}")
        return []

def save_results_to_csv(results, filename="hasil_crosscheck.csv"):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Nama Usaha', 'Hasil Crosscheck'])
        
        for business_name, _, found in results:
            status = "Ditemukan" if found else "Tidak Ditemukan"
            writer.writerow([business_name, status])
    
    print(f"Hasil crosscheck disimpan di {filename}")

if __name__ == "__main__":
    # Baca data usaha dari file
    businesses = load_businesses_from_file("bisnis.txt")
    results = []
    
    # Proses setiap usaha
    for query in businesses:
        # Panggil crosscheck_business untuk setiap query
        result = crosscheck_business(query) 
        results.append(result)
        
        # Output log 
        # Unpack hasil tuple (business_name, query, found)
        business_name, _, found = result 
        status_code = "1" if found else "0"
        # Ekstrak lokasi dari query asli untuk log
        _, business_location_log = extract_business_name(query)
        print(f"{status_code}   {business_name} {business_location_log}") 
    
    # Simpan hasil ke CSV
    save_results_to_csv(results)
    
    # Tampilkan ringkasan
    found_count = sum(1 for _, _, found in results if found)
    total = len(results)
    
    print(f"Selesai! Total usaha divalidasi: {total}")
    print(f"Usaha ditemukan: {found_count}")
    print(f"Usaha tidak ditemukan: {total - found_count}")
