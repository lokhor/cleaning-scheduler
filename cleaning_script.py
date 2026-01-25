import pandas as pd
import datetime
import gkeepapi
import os
from itertools import groupby

# Configuration
CSV_FILE = 'cleaning schedule.csv'
START_OF_WEEK_DAY = 0  # Monday (0) is the start of the week for assignment

def get_date_from_str(date_str):
    if pd.isna(date_str) or date_str == '' or date_str == 'nan':
        return None
    try:
        return pd.to_datetime(date_str).date()
    except:
        return None

def is_due(row, today):
    """Checks if a task is due for assignment based on frequency."""
    last_date = get_date_from_str(row['Last Assigned Date'])
    if last_date is None:
        return True
    
    freq = str(row['frequency']).lower().strip()
    days_since = (today - last_date).days
    
    if freq == 'daily':
        return True
    if freq == 'weekly':
        return days_since >= 7
    if freq == 'fortnightly':
        return days_since >= 14
    if freq == 'monthly':
        # Roughly a calendar month
        return days_since >= 28 and (today.month != last_date.month or today.year != last_date.year)
    return False

def assign_logic(df, today):
    """
    Greedy load balancing:
    1. Calculates weekly weight per Area (Daily effort * 7, others * 1).
    2. Assigns the entire Area to the eligible person with the least current load.
    """
    areas = df['Area'].unique()
    all_people = set()
    for val in df['Who can do this'].dropna():
        for p in val.split(','):
            all_people.add(p.strip())
    people = sorted(list(all_people))
    
    area_weights = {}
    area_eligibility = {}
    
    for area in areas:
        area_df = df[df['Area'] == area]
        weight = 0
        eligible = set(people)
        
        for _, row in area_df.iterrows():
            effort = row['Effort to complete in minutes']
            freq = str(row['frequency']).lower().strip()
            
            # Identify who is eligible for this area (must be able to do ALL tasks in area)
            row_people = set([p.strip() for p in str(row['Who can do this']).split(',')])
            eligible &= row_people
            
            if freq == 'daily':
                weight += effort * 7
            elif is_due(row, today):
                weight += effort
        
        area_weights[area] = weight
        area_eligibility[area] = list(eligible) if eligible else people

    # Balance load
    assignments = {p: 0 for p in people}
    area_to_person = {}
    sorted_areas = sorted(areas, key=lambda x: area_weights[x], reverse=True)
    
    for area in sorted_areas:
        valid_people = area_eligibility[area]
        best_person = min(valid_people, key=lambda p: assignments[best_person])
        area_to_person[area] = best_person
        assignments[best_person] += area_weights[area]
        
    for area, person in area_to_person.items():
        df.loc[df['Area'] == area, 'Currently Assigned To'] = person
        
    return df

def main():
    today = datetime.date.today()
    is_monday = (today.weekday() == START_OF_WEEK_DAY)
    
    # Load CSV preserving original headers (skipping first 2 decorative rows)
    try:
        with open(CSV_FILE, 'r') as f:
            header_lines = [next(f) for _ in range(2)]
        df = pd.read_csv(CSV_FILE, skiprows=2)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    # 1. Weekly Assignment (only on Mondays)
    if is_monday:
        print("Performing weekly area assignments...")
        df = assign_logic(df, today)
    
    # 2. Determine daily tasks to push to Keep
    tasks_to_push = []
    for idx, row in df.iterrows():
        person = row['Currently Assigned To']
        if pd.isna(person): continue
            
        freq = str(row['frequency']).lower().strip()
        # Daily tasks are added every day. 
        # Weekly/Fortnightly/Monthly only on Monday if they are due.
        if freq == 'daily' or (is_monday and is_due(row, today)):
            tasks_to_push.append({
                'person': person,
                'area': row['Area'],
                'activity': row['Activity'],
                'index': idx
            })
            df.at[idx, 'Last Assigned Date'] = today.isoformat()

    # Save CSV back to maintain state
    df.to_csv('temp.csv', index=False)
    with open(CSV_FILE, 'w') as f:
        f.writelines(header_lines)
        with open('temp.csv', 'r') as temp:
            f.write(temp.read())

    # 3. Google Keep Sync
    username = os.getenv('GOOGLE_USERNAME')
    password = os.getenv('GOOGLE_PASSWORD') # Master Token or App Password
    if not username or not password:
        print("Google credentials missing. Set GOOGLE_USERNAME and GOOGLE_PASSWORD.")
        return

    keep = gkeepapi.Keep()
    if not keep.login(username, password):
        print("Keep login failed.")
        return

    # Map people names to Keep Note Titles from env vars
    # E.g. NOTE_NICK="Nick's To-Do"
    for person in df['Currently Assigned To'].dropna().unique():
        env_key = f"NOTE_{person.upper().replace(' ', '_')}"
        note_name = os.getenv(env_key)
        if not note_name: continue

        notes = list(keep.find(query=note_name))
        note = notes[0] if notes else keep.createList(note_name, [])
        if not isinstance(note, gkeepapi.node.List): continue

        person_tasks = sorted([t for t in tasks_to_push if t['person'] == person], key=lambda x: x['area'])
        
        for area, tasks in groupby(person_tasks, key=lambda x: x['area']):
            area_header = f"[{area}]"
            # Find or create area header
            header_item = next((i for i in note.items if i.text == area_header), None)
            if not header_item:
                header_item = note.add(area_header, False)
            
            for t in tasks:
                # Remove existing tasks of the same name to "re-add" them (refresh daily)
                for item in [i for i in note.items if i.text == t['activity']]:
                    item.delete()
                
                new_item = note.add(t['activity'], False)
                new_item.parent = header_item

    keep.sync()
    print("Google Keep synchronization successful.")

if __name__ == "__main__":
    main()
