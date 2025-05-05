from botasaurus.browser import browser, Driver, AsyncQueueResult
from botasaurus.request import request, Request
from botasaurus.lang import Lang
import json
import re
from fuzzywuzzy import fuzz
import csv

def extract_title(html):
    return json.loads(
        html.split(";window.APP_INITIALIZATION_STATE=")[1].split(";window.APP_FLAGS")[0]
    )[5][3][2][1]

# Fungsi untuk melakukan validasi kesamaan nama usaha
def validation(business_name, compared_name):
    if not business_name or not compared_name:
        return False
    
    # Menghitung jumlah kata yang sama
    words_in_business = set(re.findall(r'\b\w+\b', business_name.lower()))
    words_in_compared = set(re.findall(r'\b\w+\b', compared_name.lower()))
    common_words = len(words_in_business.intersection(words_in_compared))
    
    # Cek validasi dengan berbagai metode
    if common_words >= 2:
        return True
    
    # Cek fuzzy ratio
    ratio = fuzz.ratio(business_name.lower(), compared_name.lower())
    if ratio >= 75:
        return True
    
    # Cek partial ratio
    partial_ratio = fuzz.partial_ratio(business_name.lower(), compared_name.lower())
    if partial_ratio >= 90:
        return True
    
    return False

# Ekstrak nama usaha dari query
def extract_business_name(query):
    match = re.search(r'(.+?)\s+(?:Kabupaten|Kota)\s+', query)
    if match:
        return match.group(1).strip()
    return query

@request(parallel=5, async_queue=True, max_retry=5, output=None)
def scrape_place_title(request: Request, link, metadata):
    business_name = metadata["business_name"]
    cookies = metadata["cookies"]
    
    try:
        html = request.get(link, cookies=cookies, timeout=12).text
        title = extract_title(html)
        
        if title:
            is_found = validation(business_name, title)
            return (link, title, is_found)
        return (link, None, False)
    except Exception as e:
        print(f"Error scraping {link}: {e}")
        return (link, None, False)

@browser(block_images_and_css=True,
         output=None,
         reuse_driver=True,
         wait_for_complete_page_load=True,
         lang=Lang.Indonesian)
def crosscheck_business(driver: Driver, query):
    business_name = extract_business_name(query)
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    
    try:
        driver.google_get(search_url, accept_google_cookies=True)
        h1_text = driver.get_text("h1")
        
        # Jika h1 berisi kata "hasil", ini halaman list view
        if h1_text and ("hasil" in h1_text.lower() or "results" in h1_text.lower()):
            # Proses halaman list view
            links = driver.get_all_links('[role="feed"] > div > div > a')
            links = links[:5]
            
            if not links:
                return (business_name, query, False)
            
            # Proses link menggunakan request paralel
            scrape_place_obj: AsyncQueueResult = scrape_place_title()
            cookies = driver.get_cookies_dict()
            
            # Tambahkan links ke antrian async
            scrape_place_obj.put(links, metadata={"business_name": business_name, "cookies": cookies})
            
            # Dapatkan hasil
            results = scrape_place_obj.get()

            # Proses results yang berupa list datar (setiap 3 elemen adalah satu hasil)
            for i in range(0, len(results), 3):
                if i+2 < len(results):  # Pastikan kita punya 3 elemen lengkap
                    link = results[i]
                    title = results[i+1]
                    is_found = results[i+2]
                    
                    if is_found:
                        return (business_name, query, True)

            # Jika sampai di sini, tidak ada yang ditemukan
            return (business_name, query, False)
        else:
            # Proses halaman profile bisnis
            is_found = validation(business_name, h1_text)
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
        print(f"{status_code}   {business_name} {query.replace(business_name, '')}")
    
    # Simpan hasil ke CSV
    save_results_to_csv(results)
    
    # Tampilkan ringkasan
    found_count = sum(1 for _, _, found in results if found)
    total = len(results)
    
    print(f"Selesai! Total usaha divalidasi: {total}")
    print(f"Usaha ditemukan: {found_count}")
    print(f"Usaha tidak ditemukan: {total - found_count}")
