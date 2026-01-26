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
    if pd.isna(date_str) or date_str == '' or str(date_str).lower() == 'nan': return None
    try: return pd.to_datetime(date_str).date()
    except: return None

def is_due(row, today):
    last_assigned = get_date_from_str(row['Last Assigned Date'])
    freq = str(row['frequency']).lower().strip()
    if freq == 'daily': return True, "Daily"
    if last_assigned is None: return True, "New"
    days_since = (today - last_assigned).days
    if freq == 'weekly' and days_since >= 7: return True, "Weekly"
    if freq == 'fortnightly' and days_since >= 14: return True, "Fortnightly"
    if freq == 'monthly' and days_since >= 28: return True, "Monthly"
    return False, "Not due"

def assign_logic(df, today):
    debug_print("--- Shuffling Areas (Monday) ---")
    all_people = set()
    for val in df['Who can do this'].dropna():
        for p in val.split(','): all_people.add(p.strip())
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
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            top_lines = [next(f) for _ in range(2)]
        df = pd.read_csv(CSV_FILE, skiprows=2)
    except Exception as e:
        print(f"Error: {e}"); return

    # Sort CSV by Area
    df = df.sort_values(by=['Area', 'Activity']).reset_index(drop=True)

    if is_monday:
        df = assign_logic(df, today)

    tasks_to_push = []
    for idx, row in df.iterrows():
        person = row['Currently Assigned To']
        if pd.isna(person) or person == 'nan': continue
        due, _ = is_due(row, today)
        if str(row['frequency']).lower() == 'daily' or (is_monday and due):
            tasks_to_push.append({'person': person, 'area': row['Area'], 'task': row['Activity']})
            df.at[idx, 'Last Assigned Date'] = today.isoformat()

    # Save CSV
    df.to_csv('temp.csv', index=False, encoding='utf-8')
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        f.writelines(top_lines)
        with open('temp.csv', 'r', encoding='utf-8') as t: f.write(t.read())
    os.remove('temp.csv')

    # Keep Sync
    keep = gkeepapi.Keep()
    keep.authenticate(os.getenv('GOOGLE_USERNAME'), os.getenv('GOOGLE_PASSWORD'))

    tasks_to_push.sort(key=lambda x: x['person'])
    for person, tasks in groupby(tasks_to_push, key=lambda x: x['person']):
        note_title = os.getenv(f"NOTE_{person.upper().replace(' ', '_')}")
        if not note_title: continue
        
        print(f"Syncing list for {person}...")
        notes = list(keep.find(query=note_title))
        note = notes[0] if notes else keep.createList(note_title, [])

        # 1. Clear and Sync (Fresh start)
        for item in list(note.items): item.delete()
        keep.sync() 

        # 2. Pass One: Create Headers Only
        p_tasks = list(tasks)
        header_map = {}
        distinct_areas = sorted(list(set(t['area'] for t in p_tasks)))
        
        for area in distinct_areas:
            h_text = f"--- {area.upper()} ---"
            header_map[area] = note.add(h_text, False)
        
        # Intermediate sync to bake headers into Keep's DB
        keep.sync() 

        # 3. Pass Two: Attach Tasks to Headers
        for st in p_tasks:
            clean_text = str(st['task']).replace('\n', ' ').strip()
            item = note.add(clean_text, False)
            # Explicit parenting + Indent command
            item.parent = header_map[st['area']]
            item.indent() 

        keep.sync()
    print("Done!")

if __name__ == "__main__":
    main()
