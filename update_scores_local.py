import requests
import pandas as pd
import logging
import gspread
from google.oauth2.service_account import Credentials

# === Parameters ===
URL = 'https://www.espn.com/golf/leaderboard/_/tournamentId/401580355'
GOOGLE_SHEET_NAME = 'Golf_Majors_Gamblor'
SHEET_NAME = 'TOURNAMENT_LEADERBOARDS'
PAR = 70
COLUMN_OFFSET = 14  # 0-based index: Column 'O'
SCORE_COL_START = 15  # 0-based index: Column 'P'
BLOCK_SIZE = 7
PARTICIPANT_START_ROW = 229  # 0-based: Excel 230
ROUND_COLS = ['R1', 'R2', 'R3', 'R4']
PARTICIPANTS = ['PAT', 'TADGH / TADHG', 'HAYES', 'JOE', 'COOKE', 'MACKEY', 'FITZ']

# === Google Sheets Setup ===
SCOPE = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CREDS = Credentials.from_service_account_file('creds.json', scopes=SCOPE)
gc = gspread.authorize(CREDS)
sheet = gc.open(GOOGLE_SHEET_NAME).worksheet(SHEET_NAME)
data = sheet.get_all_values()
df = pd.DataFrame(data[1:], columns=data[0])

# === Logging setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def fetch_scores():
    logging.info(f'Fetching scores from ESPN leaderboard: {URL}')
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    try:
        response = requests.get(URL, headers=headers)
        response.raise_for_status()
        tables = pd.read_html(response.text)
        leaderboard = tables[-1]
        leaderboard.columns = [col.upper() for col in leaderboard.columns]
        return leaderboard
    except Exception as e:
        logging.error(f'Error fetching leaderboard: {e}')
        return None

def find_best_match(name, leaderboard_names):
    import difflib
    last_name = name.split(',')[0].strip().lower() if ',' in name else name.split()[-1].lower()
    best_match = None
    best_ratio = 0
    for lb_name in leaderboard_names:
        lb_last = lb_name.split(',')[0].strip().lower() if ',' in lb_name else lb_name.split()[-1].lower()
        ratio = difflib.SequenceMatcher(None, last_name, lb_last).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = lb_name
    return best_match if best_ratio > 0.6 else None

def format_score(score):
    return 'E' if score == 0 else f'+{score}' if score > 0 else str(score)

def update_sheet():
    leaderboard = fetch_scores()
    if leaderboard is None:
        return

    leaderboard_names = leaderboard['PLAYER'].tolist()
    sheet_data = sheet.get_all_values()
    
    # Reset df to current sheet data to keep sync
    global df
    df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    participant_totals_day4 = []  # To hold (participant, total_score) for day 4
    
    for p_index, participant in enumerate(PARTICIPANTS):
        start_row = PARTICIPANT_START_ROW + p_index * BLOCK_SIZE
        logging.info(f'\nProcessing {participant} (starts at row {start_row + 1})')

        day_scores = {i: [] for i in range(4)}

        for i in range(5):
            row_idx = start_row + 1 + i
            try:
                name = df.iat[row_idx - 1, COLUMN_OFFSET]
            except IndexError:
                logging.warning(f'Missing golfer name at row {row_idx + 1}')
                continue

            match = find_best_match(name, leaderboard_names)
            if not match:
                logging.warning(f'No match found for {name}')
                continue

            lb_row = leaderboard[leaderboard['PLAYER'] == match].iloc[0]
            cut = False
            cumulative_score = 0

            for d, col in enumerate(ROUND_COLS):
                if col not in leaderboard.columns:
                    continue
                score = lb_row[col]
                if isinstance(score, str) and ('-' in score or score.upper() == 'CUT'):
                    cut = True
                    df.iat[row_idx - 1, SCORE_COL_START + d] = 'CUT'
                else:
                    try:
                        s = int(score)
                        over_under = s - PAR
                        cumulative_score += over_under
                        df.iat[row_idx - 1, SCORE_COL_START + d] = format_score(cumulative_score)
                        day_scores[d].append(cumulative_score)
                    except:
                        df.iat[row_idx - 1, SCORE_COL_START + d] = ''
            if cut:
                for future_day in range(d + 1, 4):
                    df.iat[row_idx - 1, SCORE_COL_START + future_day] = 'CUT'

        total_row = start_row + 6
        total_day4 = None
        for d in range(4):
            scores = [v for v in day_scores[d] if isinstance(v, int)]
            if len(scores) >= 3:
                total = sum(sorted(scores)[:3])
                df.iat[total_row - 1, SCORE_COL_START + d] = format_score(total)
                logging.info(f'Day {d+1} total for {participant}: {format_score(total)}')
                if d == 3:
                    total_day4 = total
            else:
                df.iat[total_row - 1, SCORE_COL_START + d] = ''
                if d == 3:
                    total_day4 = None
        # Collect participant totals for day 4 to sort winners later
        if total_day4 is not None:
            participant_totals_day4.append((participant, total_day4))

    # Sort participants by best total on day 4 (lowest first)
    rankings = sorted(participant_totals_day4, key=lambda x: x[1])  # (participant, score)

    # Add winner's name only (no score) two rows above first participant, keep "WINNER:" prefix
    winner_cell_row = PARTICIPANT_START_ROW - 2  # 0-based index
    winner_cell_col = COLUMN_OFFSET

    current_value = df.iat[winner_cell_row - 1, winner_cell_col]  # Check existing cell text
    winner_name = rankings[0][0] if rankings else ""

    if current_value and "WINNER:" in current_value.upper():
        df.iat[winner_cell_row - 1, winner_cell_col] = f"WINNER: {winner_name}"
    else:
        df.iat[winner_cell_row - 1, winner_cell_col] = f"WINNER: {winner_name}"

    # Write 1st, 2nd, 3rd with name and score below participants
    display_start_row = PARTICIPANT_START_ROW + BLOCK_SIZE * len(PARTICIPANTS) + 2  # Adjust as needed
    rank_suffix = ['ST', 'ND', 'RD']
    for i, (name, score) in enumerate(rankings[:3]):
        rank_str = f"{i+1}{rank_suffix[i]}"
        display_text = f"{rank_str}: {name} ({format_score(score)})"
        df.iat[display_start_row - 1 + i, winner_cell_col] = display_text

    # Push back to Google Sheets
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    logging.info('\n✅ Google Sheet updated!')

if __name__ == '__main__':
    update_sheet()
