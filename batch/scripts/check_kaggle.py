import kaggle


def check_kaggle_api():
    try:
        kaggle.api.authenticate()
        return True
    except Exception:
        return False