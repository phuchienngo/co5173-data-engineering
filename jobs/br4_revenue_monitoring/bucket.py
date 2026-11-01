def bucket_revenue(revenue):
    if revenue is None:
        return "unknown"
    if revenue < 2000:
        return "low"
    if revenue < 8000:
        return "medium"
    return "high"
