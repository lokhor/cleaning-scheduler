import pandas as pd
import os

CSV_FILE = 'cleaning schedule.csv'

def reset_csv():
    # 1. Preserve the text headers
    try:
        with open(CSV_FILE, 'r') as f:
            headers = [next(f) for _ in range(2)]
    except FileNotFoundError:
        print("CSV file not found!")
        return

    # 2. Load the data
    df = pd.read_csv(CSV_FILE, skiprows=2)
    print(f"Loaded {len(df)} rows.")

    # 3. Wipe the dates
    # Setting this to empty makes the script treat them as "Never Assigned"
    df['Last Assigned Date'] = ''
    
    # Optional: If you want to force a re-shuffle of who does what, uncomment this:
    # df['Currently Assigned To'] = ''

    # 4. Save it back
    df.to_csv('temp.csv', index=False)
    with open(CSV_FILE, 'w', newline='') as f:
        f.writelines(headers)
        with open('temp.csv', 'r') as t:
            f.write(t.read())
    os.remove('temp.csv')

    print("--- RESET COMPLETE ---")
    print("All tasks are now marked as 'Due'. Run cleaning_script.py now.")

if __name__ == "__main__":
    reset_csv()
