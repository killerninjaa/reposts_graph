import json
import pandas as pd

def main():
    print("Чтение Excel файла с именами каналов...")
    df_channels = pd.read_excel('processed_channel_usernames_small.xlsx')
    
    valid_sources = set(df_channels['Channel Username'].dropna().values)
    print(f"Найдено {len(valid_sources)} уникальных каналов в Excel файле")
    
    print("Чтение JSON файла с репостами...")
    with open('forwards_graph.json', 'r', encoding='utf-8') as f:
        forwards_data = json.load(f)
    
    print("Фильтрация записей...")
    filtered_forwards = []
    for item in forwards_data:
        if item['source'] in valid_sources:
            filtered_forwards.append(item)
    
    print(f"Найдено {len(filtered_forwards)} записей с source из Excel файла")
    
    print("Сохранение отфильтрованных записей в filtered_forwards.json...")
    with open('filtered_forwards.json', 'w', encoding='utf-8') as f:
        json.dump(filtered_forwards, f, ensure_ascii=False, indent=2)
    
    print("Сбор уникальных target каналов...")
    unique_targets = set()
    for item in filtered_forwards:
        unique_targets.add(item['target'])
    
    print(f"Найдено {len(unique_targets)} уникальных target каналов")
    
    print("Проверка на совпадение с исходным списком каналов...")
    filtered_targets = unique_targets - valid_sources
    excluded_count = len(unique_targets) - len(filtered_targets)
    
    if excluded_count > 0:
        print(f"ВНИМАНИЕ: {excluded_count} target-каналов исключены, так как они присутствуют в исходном списке")
    else:
        print("Совпадающих target-каналов с исходным списком не найдено")
    
    print(f"Будет сохранено {len(filtered_targets)} уникальных target-каналов")
    
    print("Сохранение target каналов в target_channels.xlsx...")
    df_targets = pd.DataFrame(list(filtered_targets), columns=['Target Channel'])
    df_targets.to_excel('target_channels.xlsx', index=False)
    
    print("Готово! Результаты сохранены в filtered_forwards.json и target_channels.xlsx")

main()