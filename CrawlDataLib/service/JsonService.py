import os
import json
import csv
import sys

@staticmethod
def save_data_to_json(data, dir_path="./", file_name="data.json"):
    """
    Saves a list of dictionaries to a JSON file in the specified directory.
    
    :param data: List of dictionaries to save.
    :param dir_path: Directory path where the JSON file should be saved (default: current directory).
    :param file_name: Name of the JSON file (default: 'data.json').
    :return: Full path of the saved JSON file.
    """
    try:
        # Ensure the directory exists
        os.makedirs(dir_path, exist_ok=True)

        # Full path of the JSON file
        file_path = os.path.join(dir_path, file_name)

        # Write data to JSON file
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)

        print(f"Data successfully saved to {file_path}")
        return file_path

    except Exception as e:
        print(f"Error saving JSON file: {e}")
        return None

@staticmethod
def append_data_to_json(data, file_path="data.json"):
    """
    Appends data to an existing JSON file. If the file does not exist or is empty, it creates a new one.
    
    :param data: List of dictionaries to append.
    :param file_path: Full path to the JSON file (default: 'data.json').
    :return: Full path of the saved JSON file.
    """
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Check if file exists and has data
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            # Read existing data
            with open(file_path, "r", encoding="utf-8") as json_file:
                try:
                    existing_data = json.load(json_file)
                    if not isinstance(existing_data, list):  # Ensure it's a list
                        existing_data = []
                except json.JSONDecodeError:
                    existing_data = []  # Handle corrupted JSON
        else:
            existing_data = []

        # Append new data
        if isinstance(data, list):
            existing_data.extend(data)
        else:
            existing_data.append(data)

        # Write updated data back to the file
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(existing_data, json_file, indent=4, ensure_ascii=False)

        print(f"Data successfully appended to {file_path}")
        return file_path

    except Exception as e:
        print(f"Error appending to JSON file: {e}")
        return None

    
@staticmethod
def overwrite_data_to_json(data, dir_path="./", file_name="data.json"):
    """
    Overwrites an existing JSON file with new data. If the file does not exist, it creates a new one.
    
    :param data: List of dictionaries to save.
    :param dir_path: Directory path where the JSON file should be saved (default: current directory).
    :param file_name: Name of the JSON file (default: 'data.json').
    :return: Full path of the saved JSON file.
    """
    try:
        # Ensure the directory exists
        os.makedirs(dir_path, exist_ok=True)

        # Full path of the JSON file
        file_path = os.path.join(dir_path, file_name)

        # Write (overwrite) data to JSON file
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)

        print(f"Data successfully saved (overwritten) to {file_path}")
        return file_path

    except Exception as e:
        print(f"Error saving JSON file: {e}")
        return None

@staticmethod
def print_json_file(file_path):
    """
    Reads and prints the contents of a JSON file.
    
    :param file_path: Full path to the JSON file.
    """
    try:
        # Check if the file exists
        if not os.path.exists(file_path):
            print(f"Error: File '{file_path}' does not exist.")
            return

        # Read and print JSON data
        with open(file_path, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)
            print(json.dumps(data, indent=4, ensure_ascii=False))  # Pretty print JSON

    except json.JSONDecodeError:
        print(f"Error: The file '{file_path}' is not a valid JSON file.")
    except Exception as e:
        print(f"Error reading JSON file: {e}")

@staticmethod
def json_file_exists(file_path):
    """
    Checks if a JSON file exists

    :param file_path: Full path to the JSON file.
    :return: True if the file exists, False otherwise.
    """
    return os.path.exists(file_path)

@staticmethod
def json_to_dict_list(file_path):
    """
    Reads a JSON file and converts its contents into a list of dictionaries.

    :param file_path: Full path to the JSON file.
    :return: List of dictionaries if successful, otherwise None.
    """
    try:
        # Check if the file exists
        if not os.path.exists(file_path):
            print(f"Error: File '{file_path}' does not exist.")
            return None

        # Read JSON data
        with open(file_path, "r", encoding="utf-8") as json_file:
            data = json.load(json_file)

        # Ensure data is a list of dictionaries
        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            return data
        else:
            print(f"Error: JSON file '{file_path}' does not contain a list of dictionaries.")
            return None

    except json.JSONDecodeError:
        print(f"Error: The file '{file_path}' is not a valid JSON file.")
        return None
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return None

@staticmethod
def json_to_csv(json_file_path, csv_file_path):
    """
    Converts a JSON file to a CSV file.

    :param json_file_path: Full path to the input JSON file (must contain a list of dictionaries).
    :param csv_file_path: Full path to the output CSV file.
    :raises FileNotFoundError: If the JSON file does not exist.
    :raises JSONDecodeError: If the JSON file is invalid.
    :raises ValueError: If the JSON data is not a list of dictionaries.
    :raises Exception: For other unexpected errors.
    :return: None. Prints success message or error details.
    """
    try:
        # Read JSON file
        with open(json_file_path, 'r', encoding='utf-8-sig') as json_file:
            data = json.load(json_file)
        
        # Check if data is a list of dictionaries
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise ValueError("JSON data must be a list of dictionaries")
        
        # Get headers from the first dictionary
        headers = list(data[0].keys())
        
        # Write to CSV file
        with open(csv_file_path, 'w', encoding='utf-8', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
            
        print(f"Successfully converted {json_file_path} to {csv_file_path}")
        
    except FileNotFoundError:
        print(f"Error: File {json_file_path} not found")
    except json.JSONDecodeError:
        print("Error: Invalid JSON format")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python json_to_csv.py input.json output.csv")
    else:
        json_to_csv(sys.argv[1], sys.argv[2])



