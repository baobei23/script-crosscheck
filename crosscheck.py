from botasaurus.browser import browser, Driver, AsyncQueueResult
from botasaurus.request import request, Request
from botasaurus.lang import Lang
import json
import re
from fuzzywuzzy import fuzz
import csv

# Modifikasi untuk mengekstrak nama dan lokasi dari list view
def extract_list_view(html):
    data_string = json.loads(
        html.split(";window.APP_INITIALIZATION_STATE=")[1].split(";window.APP_FLAGS")[0]
    )[9][0] # Target data nama dan alamat

    # Pisahkan nama dan alamat berdasarkan " · "
    parts = data_string.split(" · ")
    compared_name = parts[0].strip() if parts else None
    address_text = parts[1].strip() if len(parts) > 1 else None

    compared_location = None
    if address_text:
        # Ekstrak Kabupaten/Kota dari alamat
        # Pola ini mencari "Kabupaten " atau "Kota " diikuti oleh nama lokasi sampai koma atau akhir string
        loc_match = re.search(r'(?:Kabupaten|Kota)\s+([^\s,]+(?:\s+[^\s,]+)*)', address_text, re.IGNORECASE)
        if loc_match:
            compared_location = loc_match.group(1).strip()
        
    return compared_name, compared_location

# Modifikasi fungsi validasi untuk memasukkan pengecekan lokasi
def validation(business_name, compared_name, business_location, compared_location):
    if not business_name or not compared_name:
        return False # Nama tidak boleh kosong

    # Tahap 1: Validasi Nama
    name_match = False
    
    # Cek fuzzy ratio
    ratio = fuzz.ratio(business_name.lower(), compared_name.lower())
    if ratio >= 75:
        name_match = True
    else:
        # Cek partial ratio
        partial_ratio = fuzz.partial_ratio(business_name.lower(), compared_name.lower())
        if partial_ratio >= 90:
            name_match = True

    # Jika validasi nama gagal, langsung return False
    if not name_match:
        return False

    # Tahap 2: Validasi Lokasi (hanya jika nama cocok)
    if not business_location or not compared_location:
        # Jika salah satu lokasi tidak ada, anggap tidak cocok (atau bisa diubah sesuai kebutuhan)
        return False 
        
    # Cek apakah lokasi bisnis ada di dalam lokasi pembanding (case-insensitive)
    if business_location.lower() in compared_location.lower():
        return True # Nama cocok DAN Lokasi cocok

    return False # Nama cocok TAPI Lokasi tidak cocok

# Ekstrak nama usaha dari query
def extract_business_name(query):
    match = re.search(r'(.+?)\s+(?:Kabupaten|Kota)\s+', query)
    if match:
        return match.group(1).strip()
    return query

# Fungsi untuk mengekstrak lokasi (Kabupaten/Kota) dari query lengkap
def extract_location_from_query(query):
    # Pola ini mencari bagian setelah nama bisnis yang diawali "Kabupaten" atau "Kota"
    loc_match = re.search(r'(?:Kabupaten|Kota)\s+(.+)', query, re.IGNORECASE)
    if loc_match:
        sub_loc_match = re.search(r'(?:Kabupaten|Kota)\s+([^\s,]+(?:\s+[^\s,]+)*)', query, re.IGNORECASE)
        if sub_loc_match:
            return sub_loc_match.group(1).strip()
    return None # Jika format tidak sesuai

@request(parallel=5, async_queue=True, max_retry=5, output=None)
def scrape_place_title(request: Request, link, metadata):
    business_name = metadata["business_name"]
    business_location = metadata["business_location"] 
    cookies = metadata["cookies"]
    
    try:
        html = request.get(link, cookies=cookies, timeout=12).text
        # Extract_list_view sekarang mengembalikan nama dan lokasi
        compared_name, compared_location = extract_list_view(html) 
        
        if compared_name: # Cukup cek nama, validasi lengkap di fungsi validation
            is_found = validation(business_name, compared_name, business_location, compared_location)
            # Kembalikan 4 nilai
            return (link, compared_name, compared_location, is_found) 
        # Kembalikan 4 nilai meskipun gagal ekstrak
        return (link, None, None, False) 
    except Exception as e:
        print(f"Error scraping {link}: {e}")
        # Kembalikan 4 nilai meskipun error
        return (link, None, None, False) 

@browser(block_images_and_css=True,
         output=None,
         reuse_driver=True,
         wait_for_complete_page_load=True,
         lang=Lang.Indonesian)
def crosscheck_business(driver: Driver, query):
    business_name = extract_business_name(query)
    # Ekstrak lokasi bisnis dari query awal
    business_location = extract_location_from_query(query) 
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    
    try:
        driver.google_get(search_url, accept_google_cookies=True)
        
        h1_text = ""
        try: # Coba dapatkan H1 (nama usaha di halaman profil)
            h1_text = driver.get_text("h1")
        except Exception:
            pass # Biarkan kosong jika tidak ada H1

        # Cek apakah ini halaman list view atau profil
        is_list_view = False
        if not h1_text or ("hasil" in h1_text.lower() or "results" in h1_text.lower()):
            is_list_view = True

        if is_list_view:
            # --- Proses Halaman List View ---
            links = driver.get_all_links('[role="feed"] > div > div > a')
            links = links[:5]
            
            if not links:
                return (business_name, query, False)
            
            scrape_place_obj: AsyncQueueResult = scrape_place_title()
            cookies = driver.get_cookies_dict()
            
            # Kirim business_location ke metadata
            scrape_place_obj.put(links, metadata={"business_name": business_name, "business_location": business_location, "cookies": cookies}) 
            
            results = scrape_place_obj.get()

            # Proses results (list datar, sekarang 4 elemen per hasil)
            for i in range(0, len(results), 4): # Iterasi per 4 elemen
                if i+3 < len(results):  # Pastikan ada 4 elemen
                    link = results[i]
                    compared_name = results[i+1]
                    compared_location = results[i+2]
                    is_found = results[i+3] # Ambil status is_found
                    
                    # Validasi ulang tidak perlu karena sudah dilakukan di scrape_place_title
                    if is_found:
                        return (business_name, query, True)

            return (business_name, query, False) # Tidak ditemukan di 5 link teratas
        else:
            # --- Proses Halaman Profile ---
            compared_name = h1_text 
            compared_location = None
            # Ekstrak alamat dari halaman profil
            address_selector = 'div[data-section-id="ad"] .Io6YTe' 
            address_text = driver.get_text(address_selector)
            if address_text:
                 # Ekstrak Kabupaten/Kota dari alamat
                loc_match = re.search(r'(?:Kabupaten|Kota)\s+([^\s,]+(?:\s+[^\s,]+)*)', address_text, re.IGNORECASE)
                if loc_match:
                    compared_location = loc_match.group(1).strip()

            # Lakukan validasi lengkap (nama dan lokasi)
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
        business_name = extract_business_name(query)
        result = crosscheck_business(query)
        results.append(result)
        
        # Output log 
        status_code = "1" if result[2] else "0"
        location_part = query.replace(business_name, '').strip() # Ambil bagian lokasi untuk log
        print(f"{status_code}   {business_name} {location_part}") # Sesuaikan log jika perlu
    
    # Simpan hasil ke CSV
    save_results_to_csv(results)
    
    # Tampilkan ringkasan
    found_count = sum(1 for _, _, found in results if found)
    total = len(results)
    
    print(f"Selesai! Total usaha divalidasi: {total}")
    print(f"Usaha ditemukan: {found_count}")
    print(f"Usaha tidak ditemukan: {total - found_count}")
