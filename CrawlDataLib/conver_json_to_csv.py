import os
import service.JsonService as JsonService

def convert_json_to_csv(json_path, csv_path):
    JsonService.json_to_csv(json_path, csv_path)

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(script_dir, "data", "population.json")
    csv_path = os.path.join(script_dir, "data", "population.csv")
    
    convert_json_to_csv(json_path, csv_path)
    
if __name__ == "__main__":
    main()