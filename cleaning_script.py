import pandas as pd
import datetime
import gkeepapi
import os
from itertools import groupby
import sys

# Configuration
CSV_FILE = 'cleaning schedule.csv'
START_OF_WEEK_DAY = 0 
DEBUG = True

def debug_print(msg):
    if DEBUG: print(f"[DEBUG] {msg}")

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
    if freq == 'daily': return True, "Daily task"
    if last_assigned is None: return True, "Never assigned before"
    days_since = (today - last_assigned).days
    if freq == 'weekly' and days_since >= 7: return True, f"Weekly"
    if freq == 'fortnightly' and days_since >= 14: return True, f"Fortnightly"
    if freq == 'monthly' and days_since >= 28: return True, f"Monthly"
    return False, "Not due"

def assign_logic(df, today):
    debug_print("--- Running Assignment Logic ---")
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

    person_load = {p: 0 for p in people}
    for area in sorted(areas, key=lambda x: area_weights[x], reverse=True):
        best_p = min(area_to_eligible[area], key=lambda p: person_load[p])
        df.loc[df['Area'] == area, 'Currently Assigned To'] = best_p
        person_load[best_p] += area_weights[area]
    return df

def main():
    today = datetime.date.today()
    is_monday = (today.weekday() == START_OF_WEEK_DAY)
    
    try:
        with open(CSV_FILE, 'r') as f:
            top_lines = [next(f) for _ in range(2)]
        df = pd.read_csv(CSV_FILE, skiprows=2, dtype={'Currently Assigned To': str, 'Last Assigned Date': str})
    except Exception as e:
        print(f"Error: {e}"); return

    if is_monday:
        df = assign_logic(df, today)

    tasks_to_push = []
    for idx, row in df.iterrows():
        person = row['Currently Assigned To']
        if pd.isna(person) or person == 'nan': continue
        due, reason = is_due(row, today)
        if str(row['frequency']).lower() == 'daily' or (is_monday and due):
            tasks_to_push.append({'person': person, 'area': row['Area'], 'task': row['Activity'], 'index': idx})
            df.at[idx, 'Last Assigned Date'] = today.isoformat()

    # Save CSV updates
    df.to_csv('temp.csv', index=False)
    with open(CSV_FILE, 'w', newline='') as f:
        f.writelines(top_lines)
        with open('temp.csv', 'r') as t: f.write(t.read())
    os.remove('temp.csv')

    keep = gkeepapi.Keep()
    keep.authenticate(os.getenv('GOOGLE_USERNAME'), os.getenv('GOOGLE_PASSWORD'))

    tasks_to_push.sort(key=lambda x: x['person'])
    
    for person, tasks in groupby(tasks_to_push, key=lambda x: x['person']):
        note_title = os.getenv(f"NOTE_{person.upper().replace(' ', '_')}")
        if not note_title: continue
        
        notes = list(keep.find(query=note_title))
        note = notes[0] if notes else keep.createList(note_title, [])

        # Step 1: Wipe and Sync
        for item in list(note.items): item.delete()
        keep.sync() 

        # Step 2: Build Hierarchical List
        # Sort by Area then by CSV Order
        p_tasks = sorted(list(tasks), key=lambda x: (x['area'], x['index']))
        
        for area, subtasks in groupby(p_tasks, key=lambda x: x['area']):
            # Add Area Header (unchecked)
            header = note.add(f"--- {area.upper()} ---", False)
            
            for st in subtasks:
                # Add child item and explicitly set parent
                item_text = st['task'].strip()
                child = note.add(item_text, False)
                child.parent = header  # This triggers the childListItems logic
        
        # Step 3: Individual person sync to ensure structural order
        print(f"Syncing indented list for {person}...")
        keep.sync()

if __name__ == "__main__":
    main()
