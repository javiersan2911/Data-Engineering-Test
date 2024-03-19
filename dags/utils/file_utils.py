import json


def append_json(elements: list, filename):
    """
    This function appends a list of elements to a JSON file.

    Args:
        elements (list): The list of elements to append.
        filename (str): The path to the JSON file.

    Returns:
        None: The function does not return any values.
    """
    with open(filename, "r+") as file:
        file_data = json.load(file)
        for element in elements:
            file_data.append(element)
        file.seek(0)
        json.dump(file_data, file, indent=4)
