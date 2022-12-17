import os


def generate_file_paths(path, dates):
    """Sample path = '/home/nikhil/toptal/final_data/db/organizers' """
    final_paths = []
    for date in dates:
        target_path = f"{path}/{date}"
        try:
            target_files = os.listdir(target_path)
            for target_file in target_files:
                final_paths.append(
                    f"{target_path}/{target_file}"
                )
        except FileNotFoundError as e:
            continue

    return final_paths
