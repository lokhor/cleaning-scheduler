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
    
    # Always do daily tasks
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
        if pd.isna(person) or person == 'nan': 
            debug_print(f"Skipping Row {idx}: No person assigned.")
            continue
        
        # Check if due
        should_run, reason = is_due(row, today)
        
        # Logic: Run if Daily OR (It's Monday AND it's Due)
        # If it's Thursday, Weekly tasks won't run even if 'due' logic passes, 
        # unless you want them to run any day they are overdue?
        # Current logic: Weekly tasks ONLY push on Mondays.
        if str(row['frequency']).lower() == 'daily':
            is_go = True
        elif is_monday and should_run:
            is_go = True
        else:
            is_go = False

        if is_go:
            debug_print(f"Please Push: [{person}] {row['Activity']} ({reason})")
            tasks_to_push.append({
                'person': person, 
                'area': row['Area'], 
                'task': row['Activity'],
                'original_index': idx
            })
            # Update date in CSV memory
            df.at[idx, 'Last Assigned Date'] = today.isoformat()
        else:
            # Uncomment this if you want to see why things are NOT adding
            # debug_print(f"Skipping: [{person}] {row['Activity']} - {reason}")
            pass

    # Save CSV
    print(f"\nSaving CSV... ({len(tasks_to_push)} tasks marked for sync)")
    df.to_csv('temp.csv', index=False)
    with open(CSV_FILE, 'w', newline='') as f:
        f.writelines(top_lines)
        with open('temp.csv', 'r') as t: f.write(t.read())
    os.remove('temp.csv')

    # Authenticate
    print("\n--- Authenticating with Google Keep ---")
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
        
        print(f"\nProcessing Person: {person}")
        print(f"Looking for Note Title: '{note_title}' (Key: {env_key})")
        
        if not note_title: 
            print("SKIPPING: No note title found in Secrets.")
            continue
        
        notes = list(keep.find(query=note_title))
        if not notes:
            print(f"Note '{note_title}' not found. Creating new note.")
            note = keep.createList(note_title, [])
        else:
            note = notes[0]
            print(f"Found existing note: {note.title} (ID: {note.id})")

        # Clear existing items
        print("  - Clearing old items...")
        old_count = len(note.items)
        for item in list(note.items):
            item.delete()
        print(f"  - Deleted {old_count} items.")
        
        # Sort and Add
        p_tasks = sorted(list(tasks), key=lambda x: x['original_index'])
        
        print(f"  - Adding {len(p_tasks)} new tasks...")
        for area
