# test_synthetic.py
import time
import subprocess

year, month = 2024, 10  # starting month

def run_synthetic_ingest(y, m):
    """Run test_ingest_synthetic.py with year and month"""
    print(f"Ingesting synthetic sales data for {y}-{m:02d}...")
    subprocess.run(
        ["python", "test_ingest_synthetic.py", str(y), str(m)],
        check=True
    )

def run_pipeline():
    """Run the customer segmentation pipeline"""
    print("⚡ Running customer_segmentation_pipeline...")
    subprocess.run(
        ["python", "customer_segmentation_pipeline_test.py"],
        check=True
    )

if __name__ == "__main__":
    year, month = 2024, 10 
    while True:
        try:
            run_synthetic_ingest(year, month)
            run_pipeline()

            # increment month
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1

            print("⏳ Waiting 3 minutes before next batch...\n")
            time.sleep(3 * 60)

        except KeyboardInterrupt:
            print("Stopped by user.")
            break
        except Exception as e:
            print(f"Error occurred: {e}")
            break