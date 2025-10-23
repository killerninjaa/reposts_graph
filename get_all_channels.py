import os
import pandas as pd
import re
import glob

def process_channel_link(link):
    """Преобразует ссылку в нужный формат согласно правилам"""
    if not isinstance(link, str) or not link.strip():
        return None
    
    link = link.strip()
    
    if '#' in link and 'https://tgstat.ru/channel/' in link:
        parts = link.split('#')
        for part in parts:
            if 'tgstat.ru/channel/' in part:
                link = part.strip()
                break
    
    if 'tgstat.ru' in link:
        if '/@' in link:
            match = re.search(r'/@([\w\d_]+)', link)
            if match:
                return f"@{match.group(1)}"
        
        if '/channel/' in link and not '/@' in link:
            match = re.search(r'/channel/([^/]+)/stat', link)
            if match:
                return f"t.me/joinchat/{match.group(1)}"
    
    elif 't.me' in link:
        link = re.sub(r'^https?://', '', link)
        return f"t.me/{link.split('/', 1)[-1]}"
    
    elif 'telemetr.me' in link:
        if '/@' in link:
            match = re.search(r'/@([\w\d_]+)', link)
            if match:
                return f"@{match.group(1)}"
        elif '/joinchat/' in link:
            match = re.search(r'/joinchat/([\w\d_]+)', link)
            if match:
                return f"t.me/joinchat/{match.group(1)}"
        else:
            username = link.split('/')[-1]
            if username:
                return f"@{username}"
    
    return link

def process_all_files():
    """Обрабатывает все XLSX файлы в папке data"""
    files = glob.glob('data/*.xlsx')
    
    if not files:
        print("Не найдено XLSX файлов в папке data")
        return
    
    print(f"Найдено {len(files)} XLSX файлов для обработки")
    
    all_entries = []  
    
    for file in files:
        print(f"Обрабатываем файл: {os.path.basename(file)}")
        try:
            df = pd.read_excel(file)
            
            if "Channel Link" in df.columns:
                for link in df["Channel Link"][:10]:
                    original_link = link
                    
                    processed = process_channel_link(link)
                    
                    if processed:
                        all_entries.append((original_link, processed))
            else:
                print(f"  Предупреждение: в файле {os.path.basename(file)} нет колонки 'Channel Link'")
                
        except Exception as e:
            print(f"  Ошибка при обработке файла {os.path.basename(file)}: {str(e)}")
    
    result_df = pd.DataFrame(all_entries, columns=["Original Channel Link", "Channel Username"])
    
    result_df = result_df.drop_duplicates(subset=["Channel Username"], keep="first")
    
    output_file = "processed_channel_usernames_small.xlsx"
    result_df.to_excel(output_file, index=False)
    
    print(f"\nОбработка завершена. Найдено {len(all_entries)} каналов, {len(result_df)} уникальных.")
    print(f"Результат сохранен в файл: {output_file}")
    
    return result_df

if __name__ == "__main__":
    process_all_files()