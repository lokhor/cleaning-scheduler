import pandas as pd
import datetime
import gkeepapi
import os
import sys
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
    if last_assigned is None:
        return True
    
    freq = str(row['frequency']).lower().strip()
    days_since = (today - last_assigned).days
    
    if freq == 'daily': return True
    if freq == 'weekly': return days_since >= 7
    if freq == 'fortnightly': return days_since >= 14
    if freq == 'monthly': return days_since >= 28
    return False

def assign_logic(df, today):
    """
    Assigns entire areas to people for the week to balance load based on effort minutes.
    """
    # Identify all unique people from the CSV
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
            # Eligible people must be listed in 'Who can do this' for ALL tasks in an area
            row_people = [p.strip() for p in str(row['Who can do this']).split(',')]
            eligible &= set(row_people)
            
            # Daily tasks are weighted by 7 to reflect weekly effort
            mins = row['Effort to complete in minutes']
            weight += (mins * 7) if str(row['frequency']).lower() == 'daily' else mins
        
        area_weights[area] = weight
        area_to_eligible[area] = list(eligible) if eligible else people

    # Greedy assignment to balance load
    person_load = {p: 0 for p in people}
    for area in sorted(areas, key=lambda x: area_weights[x], reverse=True):
        # Assign to the eligible person with the current lowest load
        best_p = min(area_to_eligible[area], key=lambda p: person_load[p])
        df.loc[df['Area'] == area, 'Currently Assigned To'] = best_p
        person_load[best_p] += area_weights[area]
    
    return df

def main():
    today = datetime.date.today()
    is_monday = (today.weekday() == START_OF_WEEK_DAY)
    
    # Load CSV (skipping first two header lines)
    try:
        with open(CSV_FILE, 'r') as f:
            top_lines = [next(f) for _ in range(2)]
        df = pd.read_csv(CSV_FILE, skiprows=2)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # 1. Weekly Load Balancing (only on Mondays)
    if is_monday:
        print("Monday: Re-assigning weekly tasks...")
        df = assign_logic(df, today)

    # 2. Determine which tasks to push today
    tasks_to_push = []
    for idx, row in df.iterrows():
        person = row['Currently Assigned To']
        if pd.isna(person): continue
        
        freq = str(row['frequency']).lower().strip()
        # Daily tasks push every day; others push on Monday if due
        if freq == 'daily' or (is_monday and is_due(row, today)):
            tasks_to_push.append({
                'person': person, 
                'area': row['Area'], 
                'task': row['Activity']
            })
            df.at[idx, 'Last Assigned Date'] = today.isoformat()

    # Save updated CSV state
    df.to_csv('temp.csv', index=False)
    with open(CSV_FILE, 'w', newline='') as f:
        f.writelines(top_lines)
        with open('temp.csv', 'r') as t:
            f.write(t.read())
    os.remove('temp.csv')

    # 3. Authenticate and Sync to Google Keep
    username = os.getenv('GOOGLE_USERNAME')
    password = os.getenv('GOOGLE_PASSWORD') # Should be your Master Token (aas_et/...)
    
    keep = gkeepapi.Keep()
    try:
        # Use resume with your Master Token
        if password.startswith('aas_et') or password.startswith('oauth2rt'):
            keep.resume(username, password)
        else:
            # Fallback for App Password (may fail on first run due to security blocks)
            keep.authenticate(username, password)
    except Exception as e:
        print(f"Authentication failed: {e}")
        return

    # Group tasks by person for their respective notes
    tasks_to_push.sort(key=lambda x: x['person'])
    for person, tasks in groupby(tasks_to_push, key=lambda x: x['person']):
        # Map person name to Environment Variable for Note Title
        # e.g., 'Nick' looks for 'NOTE_NICK'
        env_key = f"NOTE_{person.upper().replace(' ', '_')}"
        note_title = os.getenv(env_key)
        if not note_title:
            print(f"No note title configured for {person} (check {env_key})")
            continue
        
        # Find or create the list note
        notes = list(keep.find(query=note_title))
        note = notes[0] if notes else keep.createList(note_title, [])
        
        # Process tasks for this person
        person_tasks = list(tasks)
        person_tasks.sort(key=lambda x: x['area'])
        
        for area, subtasks in groupby(person_tasks, key=lambda x: x['area']):
            header_text = f"--- {area} ---"
            
            # Ensure area header exists in the list
            header = next((i for i in note.items if i.text == header_text), None)
            if not header:
                header = note.add(header_text, False)
            
            for st in subtasks:
                # Remove existing instances of the task to avoid duplicates on daily refresh
                for item in [i for i in note.items if i.text == st['task']]:
                    item.delete()
                
                # Add task nested under the area header
                new_item = note.add(st['task'], False)
                new_item.parent = header
    
    keep.sync()
    print(f"Successfully synced {len(tasks_to_push)} tasks to Google Keep.")

if __name__ == "__main__":
    main()
