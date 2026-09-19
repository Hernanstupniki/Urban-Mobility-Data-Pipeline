from src.common.bronze import run_bronze

if __name__ == "__main__":
    run_bronze("zones", watermark_column="created_at")
