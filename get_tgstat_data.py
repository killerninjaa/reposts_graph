import pandas as pd
import re
import time
import json
import sys
import asyncio
import aiohttp
import os
import random
from aiohttp import ClientTimeout
from aiohttp_retry import RetryClient, ExponentialRetry

API_TOKEN = "5c4d26547f3098cd98ecc9e4dd875e09"
BASE_URL = "https://api.tgstat.ru" 

PROGRESS_FILE = "processing_progress_2_level.json"
FINAL_DATA_FILE = "forwards_graph_2_level.json"
ALL_ITEMS_FILE = "all_items_test_2_level.json"
TEMP_DATA_FILE = "forwards_graph_temp_2_level.json"
TEMP_ITEMS_FILE = "all_items_test_temp_2_level.json"

file_path = "target_channels.xlsx"
df = pd.read_excel(file_path, sheet_name=0)

df["username"] = df["Target Channel"]
channels = df["username"].dropna().unique().tolist()

print(f"Всего каналов: {len(channels)}")

def load_progress():
    """Загружает прогресс из файла, если он существует"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                progress = json.load(f)
                print(f"✅ Загружен прогресс: обработано {progress['processed']} каналов из {len(channels)}")
                return progress
        except Exception as e:
            print(f"⚠️ Ошибка загрузки прогресса: {e}")
    return {"processed": 0, "total": len(channels), "last_channel": None}

def save_progress(processed, last_channel):
    """Сохраняет текущий прогресс"""
    try:
        progress = {
            "processed": processed,
            "total": len(channels),
            "last_channel": last_channel,
            "timestamp": time.time()
        }
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"❌ Ошибка сохранения прогресса: {e}")
        return False

def load_data():
    """Загружает уже собранные данные, если они существуют"""
    all_data = []
    all_items = []
    
    if os.path.exists(TEMP_DATA_FILE):
        try:
            with open(TEMP_DATA_FILE, "r", encoding="utf-8") as f:
                all_data = json.load(f)
            print(f"✅ Загружено {len(all_data)} записей из временного файла данных")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки временных данных: {e}")
    
    if os.path.exists(TEMP_ITEMS_FILE):
        try:
            with open(TEMP_ITEMS_FILE, "r", encoding="utf-8") as f:
                all_items = json.load(f)
            print(f"✅ Загружено {len(all_items)} записей из временного файла items")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки временных items: {e}")
    
    return all_data, all_items

def save_data(all_data, all_items, channel_name=None):
    """Сохраняет данные после обработки канала"""
    try:
        with open(TEMP_DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        
        with open(TEMP_ITEMS_FILE, "w", encoding="utf-8") as f:
            json.dump(all_items, f, ensure_ascii=False, indent=2)
        
        status_msg = f"✅ Промежуточное сохранение после обработки канала {channel_name}"
        if channel_name:
            print(f"{status_msg}. Всего записей: {len(all_data)}")
        else:
            print(f"{status_msg}. Всего записей: {len(all_data)}")
        
        return True
    except Exception as e:
        print(f"❌ Ошибка промежуточного сохранения: {e}")
        return False

def finalize_data():
    """Финальное сохранение данных и очистка временных файлов"""
    try:
        if os.path.exists(TEMP_DATA_FILE):
            with open(TEMP_DATA_FILE, "r", encoding="utf-8") as f:
                all_data = json.load(f)
            with open(FINAL_DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)
            print(f"✅ Окончательное сохранение данных в {FINAL_DATA_FILE}. Всего записей: {len(all_data)}")
        
        if os.path.exists(TEMP_ITEMS_FILE):
            with open(TEMP_ITEMS_FILE, "r", encoding="utf-8") as f:
                all_items = json.load(f)
            with open(ALL_ITEMS_FILE, "w", encoding="utf-8") as f:
                json.dump(all_items, f, ensure_ascii=False, indent=2)
            print(f"✅ Окончательное сохранение items в {ALL_ITEMS_FILE}. Всего записей: {len(all_items)}")
        
        if os.path.exists(TEMP_DATA_FILE):
            os.remove(TEMP_DATA_FILE)
        if os.path.exists(TEMP_ITEMS_FILE):
            os.remove(TEMP_ITEMS_FILE)
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            
        return True
    except Exception as e:
        print(f"❌ Ошибка финального сохранения: {e}")
        return False

async def safe_request(session, url, params, retries=1, delay=1):
    """Безопасный асинхронный запрос к API с повторными попытками"""
    for attempt in range(retries):
        try:
            if attempt > 0:
                await asyncio.sleep(delay * (2 ** (attempt - 1)) + 0.1 * random.random())
                
            async with session.get(url, params=params, timeout=ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    print(f"⚠️ HTTP ошибка {resp.status} при запросе {url}")
                    continue
                    
                data = await resp.json()
                if data.get("status") == "ok" and "response" in data:
                    return data["response"]
                else:
                    print(f"⚠️ Ошибка API {url}: {data}")
        except Exception as e:
            print(f"⚠️ Ошибка запроса {url}: {e} (попытка {attempt+1})")
            await asyncio.sleep(delay * (attempt + 1))
    return None

async def fetch_post(session, post_id, semaphore):
    """Асинхронно получает информацию о посте"""
    async with semaphore: 
        return await safe_request(
            session,
            f"{BASE_URL}/posts/get",
            {"token": API_TOKEN, "postId": post_id}
        )

async def process_items_for_channel(session, items, channels, source_ch, ch, post_dates):
    """Обрабатывает items для канала с параллельными запросами к API"""
    tasks = []
    
    semaphore = asyncio.Semaphore(3) 
    
    for item in items:
        target_channel = None
        for channel in channels:
            if channel.get("id") == item.get("channelId"):
                target_channel = channel
                break
                
        if not target_channel:
            continue
            
        target = target_channel.get("username")
        if not target:
            continue
            
        if (item.get("postDate"), target) in post_dates:
            continue
        post_dates.add((item.get("postDate"), target))
        
        tasks.append((item, target_channel, asyncio.create_task(
            fetch_post(session, item.get("sourcePostId"), semaphore)
        )))
    
    results = []
    for i, (item, target_channel, task) in enumerate(tasks):
        post = await task
        if post:
            results.append((item, target_channel, post))
    
    return results

async def main():
    start_time = time.time()
    
    progress = load_progress()
    all_data, all_items = load_data()
    
    start_idx = progress["processed"]
    print(f"Начинаем обработку с канала #{start_idx + 1} из {len(channels)}")
    
    retry_options = ExponentialRetry(attempts=1)
    async with RetryClient(retry_options=retry_options) as session:
        for idx, ch in enumerate(channels[start_idx:], start=start_idx + 1):
            channel_start_time = time.time()
            print(f"\n[{idx}/{len(channels)}] Обрабатываем канал @{ch}...")
            
            source_ch = await safe_request(
                session,
                f"{BASE_URL}/channels/get",
                {"token": API_TOKEN, "channelId": ch}
            )
            if not source_ch:
                print(f"⚠️ Пропускаем канал @{ch}, так как нет данных")
                save_progress(idx, ch)
                continue
            
            offset = 0
            post_dates = set()
            channel_data_count = 0
            
            while True:
                params = {
                    "token": API_TOKEN,
                    "channelId": ch,
                    "limit": 20,
                    "offset": offset,
                    "extended": 1
                }
                response = await safe_request(
                    session,
                    f"{BASE_URL}/channels/forwards",
                    params
                )
                
                if not response:
                    break
                    
                items = response.get("items", [])
                channels_data = response.get("channels", [])
                if not items or not channels_data:
                    break
                
                all_items.extend(items)
                
                results = await process_items_for_channel(
                    session, items, channels_data, source_ch, ch, post_dates
                )
                
                # Формируем записи
                for item, channel, post in results:
                    tgstat_restrictions = source_ch.get("tgstat_restrictions") or {}
                    red_label = tgstat_restrictions.get("red_label") if isinstance(tgstat_restrictions, dict) else None
                    black_label = tgstat_restrictions.get("black_label") if isinstance(tgstat_restrictions, dict) else None

                    record = {
                        "source": ch,
                        "source_info": {
                            "link": source_ch.get("link"),
                            "title": source_ch.get("title"),
                            "peer_type": source_ch.get("peer_type"),
                            "category": source_ch.get("category"),
                            "about": source_ch.get("about"),
                            "rkn_verification": source_ch.get("rkn_verification", {}).get("status"),
                            "country": source_ch.get("country"),
                            "language": source_ch.get("language"),
                            "participants_cnt": source_ch.get("participants_count"),
                            "red_label": red_label,
                            "black_label": black_label,
                        },
                        "target": channel.get("username"),
                        "target_info": {
                            "link": channel.get("link"),
                            "title": channel.get("title"),
                            "about": channel.get("about"),
                            "participants_cnt": channel.get("participants_count"),
                        },
                        "post": {
                            "id": post.get("id"),
                            "date": item.get("postDate"),
                            "views": post.get("views"),
                            "link": item.get("postLink"),
                            "text": post.get("text"),
                        }
                    }
                    
                    all_data.append(record)
                    channel_data_count += 1
                
                offset += 20
                await asyncio.sleep(4)
            
            save_data(all_data, all_items, ch)
            save_progress(idx, ch)
            
            channel_time = time.time() - channel_start_time
            print(f"✅ Канал @{ch} обработан за {channel_time:.2f} сек. Найдено записей: {channel_data_count}")
            print(f"📊 Общее количество записей на текущий момент: {len(all_data)}")
    
    finalize_data()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n{'='*50}")
    print(f"ВСЕ ДАННЫЕ УСПЕШНО СОБРАНЫ И СОХРАНЕНЫ")
    print(f"Всего каналов обработано: {len(channels)}")
    print(f"Всего записей: {len(all_data)}")
    print(f"Время выполнения: {elapsed_time:.2f} секунд ({elapsed_time/60:.2f} минут)")
    print(f"{'='*50}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
        print("Скрипт остановлен, но все собранные данные сохранены.")
        sys.exit(1)