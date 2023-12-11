import os
import json
import csv

def flatten_json(json_obj, prefix=''):
    #Функція для вирівнювання вкладених структур JSON.
    flat_dict = {}
    for key, value in json_obj.items():
        if isinstance(value, dict):
            flat_dict.update(flatten_json(value, prefix + key + '_'))
        else:
            flat_dict[prefix + key] = value
    return flat_dict

def process_json_file(json_file_path):
    #Функція для обробки JSON-файлу: зчитує його та вирівнює структуру даних.
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)
        flattened_data = flatten_json(data)
        return flattened_data

def convert_to_csv(json_folder_path, csv_folder_path):
    #Функція для конвертації JSON-файлів у CSV та збереження результатів у відповідних файлах CSV.
    for root, dirs, files in os.walk(json_folder_path):
        for json_file_name in files:
            if json_file_name.endswith('.json'):
                json_file_path = os.path.join(root, json_file_name)
                
                # Визначаємо шлях відносно корінної папки
                relative_path = os.path.relpath(json_file_path, json_folder_path)
                csv_file_path = os.path.join(csv_folder_path, f"{os.path.splitext(relative_path)[0]}.csv")

                # Перевірка і створення каталогу для CSV-файлу, якщо його не існує
                csv_file_dir = os.path.dirname(csv_file_path)
                if not os.path.exists(csv_file_dir):
                    os.makedirs(csv_file_dir)

                # Обробка JSON-файлу та запис у CSV-файл
                flattened_data = process_json_file(json_file_path)
                with open(csv_file_path, 'w', newline='') as csv_file:
                    csv_writer = csv.DictWriter(csv_file, fieldnames=flattened_data.keys())
                    csv_writer.writeheader()
                    csv_writer.writerow(flattened_data)

def main():
    json_folder_path = "./lab3/data"
    csv_folder_path = "./lab3/csv_output"

    convert_to_csv(json_folder_path, csv_folder_path)

if __name__ == "__main__":
    main()
