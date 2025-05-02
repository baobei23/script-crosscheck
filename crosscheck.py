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
        
    # Membersihkan nama dari karakter khusus
    business_name = re.sub(r'[<>()]', '', business_name)
    compared_name = re.sub(r'[<>()]', '', compared_name)
    
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

@browser(parallel=3, output=None, reuse_driver=True, wait_for_complete_page_load=False, block_images_and_css=True, lang=Lang.Indonesian)
def crosscheck_business(driver: Driver, query):
    business_name = extract_business_name(query)
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    
    try:
        driver.google_get(search_url, accept_google_cookies=True)
        h1_text = driver.get_text("h1")
        
        # Jika h1 berisi kata "hasil", ini halaman list view
        if h1_text and "hasil" in h1_text.lower():
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
            
            # Cek apakah ada hasil yang valid
            for link, title, is_found in results:
                if is_found:
                    return (business_name, query, True)
            
            # Jika tidak ada yang valid
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
    businesses = load_businesses_from_file("bisnis.txt")
    results = crosscheck_business(businesses)

    for business_name, query, found in results:
        status_code = "1" if found else "0"
        status_text = "ditemukan" if found else "tidak ditemukan"
        print(f"{status_code} {business_name} {query.replace(business_name, '')} {status_text}")
    
    save_results_to_csv(results)

    found_count = sum(1 for _, _, found in results if found)
    total = len(results)
    print(f"Selesai! Total usaha divalidasi: {total}")
    print(f"Usaha ditemukan: {found_count}")
    print(f"Usaha tidak ditemukan: {total - found_count}")
