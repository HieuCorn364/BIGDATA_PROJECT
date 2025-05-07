import os


@staticmethod
def create_txt_file(file_name, content):
    """
    Creates a TXT file with the given name and writes the provided content to it.
    
    :param file_name: Name of the TXT file (string)
    :param content: Content to be written to the file (string)
    """
    try:
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(content)
        print(f"File '{file_name}' created successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")

@staticmethod
def add_data_to_txt_file(file_name, new_data):
    """
    Adds new data to an existing TXT file.
    
    :param file_name: Name of the TXT file (string)
    :param new_data: Data to be appended to the file (string)
    """
    try:
        with open(file_name, 'a', encoding='utf-8') as file:
            file.write('\n' + new_data)  # Appends new data and starts a new line
        print(f"Data added to file '{file_name}' successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")

@staticmethod       
def check_txt_file_exists(file_path):
    """
    Checks whether a TXT file exists in the current directory.
    
    :param file_name: Name of the TXT file (string)
    :return: True if the file exists, False otherwise
    """
    if os.path.isfile(file_path):
        return True
    else:
        return False

