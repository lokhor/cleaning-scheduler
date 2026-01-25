import pandas as pd
import datetime
import gkeepapi
import os
from itertools import groupby

# Configuration
CSV_FILE = 'cleaning schedule.csv'
START_OF_WEEK_DAY = 0  # 0 = Monday

def get_date_from_str(date_str):
    if pd.isna(date_str) or date_str == '' or str(date_str).lower() == 'nan':
        return None
    try:
        return pd.to_datetime(date_str).date()
    except:
        return None

def is_due(row, today):
    last_assigned = get_date_from_str(row['Last Assigned Date'])
    if last_assigned is None: return True
    freq = str(row['frequency']).lower().strip()
    days_since = (today - last_assigned).days
    
    if freq == 'daily': return True
    if freq == 'weekly': return days_since >= 7
    if freq == 'fortnightly': return days_since >= 14
    if freq == 'monthly': return days_since >= 28
    return False

def assign_logic(df, today):
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
        
        df = pd.read_csv(CSV_FILE, skiprows=2, dtype={
            'Currently Assigned To': str,
            'Last Assigned Date': str
        })
    except Exception as e:
        print(f"Error reading CSV: {e}"); return

    if is_monday:
        df = assign_logic(df, today)

    tasks_to_push = []
    for idx, row in df.iterrows():
        person = row['Currently Assigned To']
        if pd.isna(person) or person == 'nan': continue
        
        freq = str(row['frequency']).lower().strip()
        if freq == 'daily' or (is_monday and is_due(row, today)):
            tasks_to_push.append({
                'person': person, 
                'area': row['Area'], 
                'task': row['Activity'],
                'original_index': idx
            })
            df.at[idx, 'Last Assigned Date'] = today.isoformat()

    df.to_csv('temp.csv', index=False)
    with open(CSV_FILE, 'w', newline='') as f:
        f.writelines(top_lines)
        with open('temp.csv', 'r') as t: f.write(t.read())
    os.remove('temp.csv')

    username = os.getenv('GOOGLE_USERNAME')
    password = os.getenv('GOOGLE_PASSWORD') 
    keep = gkeepapi.Keep()
    try:
        keep.resume(username, password)
    except:
        keep.authenticate(username, password)

    tasks_to_push.sort(key=lambda x: x['person'])
    for person, tasks in groupby(tasks_to_push, key=lambda x: x['person']):
        env_key = f"NOTE_{person.upper().replace(' ', '_')}"
        note_title = os.getenv(env_key)
        if not note_title: continue
        
        notes = list(keep.find(query=note_title))
        note = notes[0] if notes else keep.createList(note_title, [])
        
        # 1. ARCHIVE: Move checked items to archive, then remove from active view
        for item in note.items:
            if item.checked:
                item.archived = True
        
        # 2. SYNC: Map existing items by text to avoid redundant deletes
        existing_items = {item.text: item for item in note.items if not item.archived}
        p_tasks = sorted(list(tasks), key=lambda x: x['original_index'])
        
        # 3. REBUILD: Clear and re-add in order (using a stable list)
        for item in list(note.items):
            item.delete()

        for area, subtasks in groupby(p_tasks, key=lambda x: x['area']):
            header = note.add(f"--- {area} ---", False)
            for st in subtasks:
                new_item = note.add(st['task'], False)
                new_item.parent = header
    
    keep.sync()
    print("Sync Successful.")

if __name__ == "__main__":
    main()
