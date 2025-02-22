import asyncio
import httpx
import csv
import json
import aiofiles
import logging
from bs4 import BeautifulSoup

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ URL yang benar setelah redirect
BASE_URL = "https://www.bmkg.go.id/cuaca/prakiraan-cuaca"

# ✅ Tambahkan header agar tidak terdeteksi sebagai bot
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}

# File penyimpanan
OUTPUT_CSV = "datasets/bmkg_cuaca.csv"
SKIPPED_FILE = "datasets/skipped.json"

async def fetch_page(url):
    """Mengambil halaman dengan delay untuk menghindari pemblokiran"""
    await asyncio.sleep(2)  # Delay agar tidak dianggap spam
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(url, headers=HEADERS, timeout=10)

            # Log URL final setelah redirect
            logging.info(f"🔄 Redirected to: {response.url}")

            if response.status_code == 200:
                return response.text
            else:
                logging.error(f"⚠️ Gagal mengambil halaman {url}: {response.status_code}")
                return None
    except Exception as e:
        logging.error(f"❌ Error saat mengambil {url}: {e}")
        return None

def parse_page(html):
    """Mengambil data dari halaman HTML"""
    soup = BeautifulSoup(html, "html.parser")
    data_list = []

    # ✅ Debug: Simpan HTML ke file sementara
    with open("debug_bmkg.html", "w", encoding="utf-8") as f:
        f.write(soup.prettify())

    # ✅ Menemukan semua kota
    kota_sections = soup.find_all("div", class_="col-md-4 col-sm-6 col-xs-12")
    if not kota_sections:
        logging.warning("⚠️ Tidak menemukan elemen yang diharapkan. Periksa HTML halaman.")

    for kota in kota_sections:
        try:
            title = kota.find("h2").text.strip() if kota.find("h2") else "Tidak diketahui"
            link = kota.find("a")["href"] if kota.find("a") else None
            if link:
                data_list.append({"title": title, "link": "https://www.bmkg.go.id" + link})
        except Exception as e:
            logging.error(f"Kesalahan saat parsing data: {e}")
            continue

    return data_list

async def save_to_csv_async(data):
    """Menyimpan hasil scraping ke CSV secara async"""
    if not data:
        logging.warning("❌ Tidak ada data yang berhasil diambil.")
        return

    async with aiofiles.open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "link"], delimiter=";")  # Gunakan ; sebagai delimiter
        await f.write(";".join(writer.fieldnames) + "\n")  # Menulis header
        for row in data:
            await f.write(f'{row["title"]};{row["link"]}\n')
    logging.info(f"✅ Data berhasil disimpan ke {OUTPUT_CSV}")

async def save_skipped_async(skipped_pages):
    """Menyimpan halaman yang gagal ke JSON secara async"""
    if skipped_pages:
        async with aiofiles.open(SKIPPED_FILE, "w", encoding="utf-8") as f:
            await f.write(json.dumps(skipped_pages, indent=4))
        logging.warning(f"⚠️ Halaman yang gagal disimpan ke {SKIPPED_FILE}")

async def main():
    logging.info("🚀 Memulai scraping BMKG...")
    skipped_pages = []
    html = await fetch_page(BASE_URL)

    if html:
        data = parse_page(html)
        if data:
            await save_to_csv_async(data)
        else:
            logging.warning("⚠️ Tidak ada data yang diproses dari halaman.")
            skipped_pages.append(BASE_URL)
    else:
        skipped_pages.append(BASE_URL)

    await save_skipped_async(skipped_pages)

# Jalankan program
if __name__ == "__main__":
    asyncio.run(main())