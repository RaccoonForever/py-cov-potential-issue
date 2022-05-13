def get_bucket_name(full_path):
    splitted_path = full_path.split('/')
    for name in splitted_path:
        if name in ['s3a:', '', 's3:']:
            continue
        return name
    return None
