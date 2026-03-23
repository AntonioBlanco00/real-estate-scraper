import subprocess
import time
import sys

def run_step(script_name, step_description):
    print(f"\n{'='*60}")
    print(f"STEP: {step_description}")
    print(f"Executing: {script_name}")
    print(f"{'='*60}\n")
    
    try:
        subprocess.run([sys.executable, script_name], check=True)
        print(f"\n SUCCESS: {script_name} finished perfectly.")
    except subprocess.CalledProcessError as e:
        print(f"\n CRITICAL ERROR: {script_name} failed. Halting the pipeline.")
        exit(1)

if __name__ == "__main__":
    print("\n REAL ESTATE ETL & AI PIPELINE ORCHESTRATOR")
    print("Starting automated execution of all phases...\n")

    # Phase 0: Database Setup/Verification
    run_step("database.py", "Initialize and Verify Database Schema")
    
    # Phase 1: Web Scraping & Database UPSERT
    run_step("scraper.py", "Core Data Extraction and Database Load (SQLite)")
    
    print("\n Pausing for 3 seconds before AI Enrichment...")
    time.sleep(3)
    
    # Phase 2: AI Enrichment via OpenAI
    run_step("ai_enrichment.py", "AI Deep Scraping & JSON Parsing")
    
    print("\n ALL PIPELINE PHASES EXECUTED SUCCESSFULLY!")
    print(" The database 'properties.db' is completely updated and ready for Power BI.")