import pandas as pd
import datetime
import gkeepapi
import os
from itertools import groupby
import sys

# Configuration
CSV_FILE = 'cleaning schedule.csv'
START_OF_WEEK_DAY = 0  # 0 = Monday
DEBUG = True # Forces extra printing

def debug_print(msg):
    if DEBUG:
        print(f"[DEBUG] {msg}")

def get_date_from_str(date_str):
    if pd.isna(date_str) or date_str == '' or str(date_str).lower() == 'nan':
        return None
    try:
        return pd.to_datetime(date_str).date()
    except:
        return None

def is_due(row, today):
    last_assigned = get_date_from_str(row['Last Assigned Date'])
    freq = str(row['frequency']).lower().strip()
    
    if freq == 'daily': 
        return True, "Daily task"
        
    if last_assigned is None: 
        return True, "Never assigned before"
    
    days_since = (today - last_assigned).days
    
    if freq == 'weekly' and days_since >= 7: return True, f"Weekly (Last: {days_since} days ago)"
    if freq == 'fortnightly' and days_since >= 14: return True, f"Fortnightly (Last: {days_since} days ago)"
    if freq == 'monthly' and days_since >= 28: return True, f"Monthly (Last: {days_since} days ago)"
    
    return False, f"Not due yet ({freq}, Last: {days_since} days ago)"

def assign_logic(df, today):
    debug_print("--- Running Assignment Logic (Monday) ---")
    all_people = set()
    for val in df['Who can do this'].dropna():
        for p in val.split(','):
            all_people.add(p.strip())
    people = sorted(list(all_people))
    
    areas = df['Area'].unique()
    area_weights = {}
    area_to_eligible = {}

    for area in areas:
        area_df = df[df['Area'] == area]
        weight = 0
        eligible = set(people)
        for _, row in area_df.iterrows():
            row_people = [p.strip() for p in str(row['Who can do this']).split(',')]
            eligible &= set(row_people)
            mins = row['Effort to complete in minutes']
            weight += (mins * 7) if str(row['frequency']).lower() == 'daily' else mins
        
        area_weights[area] = weight
        area_to_eligible[area] = list(eligible) if eligible else people
        debug_print(f"Area '{area}' Weight: {weight}, Eligible: {area_to_eligible[area]}")

    person_load = {p: 0 for p in people}
    for area in sorted(areas, key=lambda x: area_weights[x], reverse=True):
        best_p = min(area_to_eligible[area], key=lambda p: person_load[p])
        df.loc[df['Area'] == area, 'Currently Assigned To'] = best_p
        person_load[best_p] += area_weights[area]
        debug_print(f"Assigned '{area}' to {best_p} (New Load: {person_load[best_p]})")
    
    return df

def main():
    today = datetime.date.today()
    is_monday = (today.weekday() == START_OF_WEEK_DAY)
    print(f"--- Script Start: {today} (Monday={is_monday}) ---")
    
    try:
        with open(CSV_FILE, 'r') as f:
            top_lines = [next(f) for _ in range(2)]
        
        df = pd.read_csv(CSV_FILE, skiprows=2, dtype={
            'Currently Assigned To': str,
            'Last Assigned Date': str
        })
        print(f"Loaded CSV with {len(df)} rows.")
    except Exception as e:
        print(f"CRITICAL ERROR reading CSV: {e}")
        return

    if is_monday:
        df = assign_logic(df, today)

    tasks_to_push = []
    print("\n--- Checking Task Due Dates ---")
    for idx, row in df.iterrows():
        person = row['Currently Assigned To']
        if pd.isna(person) or person == 'nan': continue
        
        should_run, reason = is_due(row, today)
        
        # Logic: Daily always goes. Others only go on Mondays if due.
        if str(row['frequency']).lower() == 'daily':
            is_go = True
        elif is_monday and should_run:
            is_go = True
        else:
            is_go = False

        if is_go:
            debug_print(f"Please Push: [{person}] {row['Area']} - {row['Activity']}")
            tasks_to_push.append({
                'person': person, 
                'area': row['Area'], 
                'task': row['Activity'],
                'original_index': idx
            })
            df.at[idx, 'Last Assigned Date'] = today.isoformat()

    # Save CSV
    df.to_csv('temp.csv', index=False)
    with open(CSV_FILE, 'w', newline='') as f:
        f.writelines(top_lines)
        with open('temp.csv', 'r') as t: f.write(t.read())
    os.remove('temp.csv')

    # Authenticate
    username = os.getenv('GOOGLE_USERNAME')
    password = os.getenv('GOOGLE_PASSWORD') 
    keep = gkeepapi.Keep()
    
    try:
        keep.authenticate(username, password)
        print("Authentication Successful.")
    except Exception as e:
        print(f"Authentication FAILED: {e}")
        return

    # Sync Logic
    tasks_to_push.sort(key=lambda x: x['person'])
    
    for person, tasks in groupby(tasks_to_push, key=lambda x: x['person']):
        env_key = f"NOTE_{person.upper().replace(' ', '_')}"
        note_title = os.getenv(env_key)
        if not note_title: continue
        
        print(f"\nProcessing Person: {person}")
        notes = list(keep.find(query=note_title))
        note = notes[0] if notes else keep.createList(note_title, [])

        # 1. CLEAR and Intermediate Sync (Crucial for correct nesting)
        for item in list(note.items):
            item.delete()
        keep.sync() 

        # 2. Sort tasks by Area then by Original Index
        p_tasks = sorted(list(tasks), key=lambda x: (x['area'], x['original_index']))
        
        # 3. Add to Keep with Header Grouping
        for area, subtasks in groupby(p_tasks, key=lambda x: x['area']):
            header = note.add(f"--- {area} ---", False)
            for st in subtasks:
                new_item = note.add(st['task'], False)
                new_item.parent = header # Indent under Area
        
        # Sync per person to ensure structural integrity
        keep.sync()
        print(f"Sync for {person} completed.")

    print("\n--- Final Sync Finished ---")

if __name__ == "__main__":
    main()
