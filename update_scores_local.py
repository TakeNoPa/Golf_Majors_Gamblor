import requests
import pandas as pd
import logging
import gspread
from google.oauth2.service_account import Credentials

# === Parameters ===
URL = 'https://www.espn.com/golf/leaderboard/_/tournamentId/401703515'
GOOGLE_SHEET_NAME = 'Golf_Majors_Gamblor'
SHEET_NAME = 'TOURNAMENT_LEADERBOARDS'
PAR = 70
COLUMN_OFFSET = 14  # Column 'O'
SCORE_COL_START = 15  # Column 'P'
BLOCK_SIZE = 7
PARTICIPANT_START_ROW = 229  # Row 230 in Excel
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
    headers = {'User-Agent': 'Mozilla/5.0'}
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

def is_round_in_progress(leaderboard, round_col):
    if round_col not in leaderboard.columns:
        return False
    valid_scores = leaderboard[round_col].dropna().astype(str)
    return any(val.strip().isdigit() for val in valid_scores)

def is_round_complete_for_golfer(lb_row, round_index):
    col = ROUND_COLS[round_index]
    val = lb_row.get(col, '')
    try:
        int(val)
        return True
    except:
        return False

def update_sheet():
    leaderboard = fetch_scores()
    if leaderboard is None:
        return

    leaderboard.columns = [col.upper() for col in leaderboard.columns]
    leaderboard_names = leaderboard['PLAYER'].tolist()
    sheet_data = sheet.get_all_values()
    global df
    df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    participant_totals_day4 = []
    found_any_scores = False  # Flag to track if we processed any real scores

    for p_index, participant in enumerate(PARTICIPANTS):
        start_row = PARTICIPANT_START_ROW + p_index * BLOCK_SIZE
        logging.info(f'\nProcessing {participant} (starts at row {start_row + 1})')

        day_scores = {i: [] for i in range(4)}

        for i in range(5):  # Each participant has 5 golfers
            row_idx = start_row + 1 + i
            try:
                name = df.iat[row_idx - 1, COLUMN_OFFSET]
            except IndexError:
                logging.warning(f'Missing golfer name at row {row_idx + 1}')
                continue

            match = find_best_match(name, leaderboard_names)
            logging.info(f'Matched "{name}" -> "{match}"')
            if not match:
                logging.warning(f'No match found for {name}')
                continue

            lb_row = leaderboard[leaderboard['PLAYER'] == match].iloc[0]
            cut = False
            cumulative_score = 0
            last_valid_score = None

            for d, col in enumerate(ROUND_COLS):
                if col not in leaderboard.columns:
                    continue

                val = lb_row.get(col, '')
                use_fallback = False

                # Check if round is incomplete (e.g., '--', empty) and previous round is complete
                if (not is_round_complete_for_golfer(lb_row, d)) and (
                    d == 0 or is_round_complete_for_golfer(lb_row, d - 1)
                ):
                    use_fallback = True

                # Try using actual round score
                try:
                    s = int(val)
                    over_under = s - PAR
                    cumulative_score += over_under
                    df.iat[row_idx - 1, SCORE_COL_START + d] = format_score(cumulative_score)
                    last_valid_score = over_under
                    day_scores[d].append(cumulative_score)
                    logging.info(f'{name} {col}: {s} → {format_score(over_under)}')
                    found_any_scores = True
                    continue
                except:
                    pass

                # CUT handling for R3 and R4
                if isinstance(val, str) and val.upper() == 'CUT' and d >= 2:
                    df.iat[row_idx - 1, SCORE_COL_START + d] = 'CUT'
                    cut = True
                    logging.info(f'{name} {col}: CUT')
                    break

                # Use fallback if needed
                if use_fallback:
                    fallback = lb_row.get('SCORE')
                    if isinstance(fallback, str):
                        try:
                            fallback = fallback.strip().upper()
                            if fallback == 'E':
                                over_under = 0
                            elif fallback.startswith('+') or fallback.startswith('-'):
                                over_under = int(fallback)
                            else:
                                over_under = int(fallback)

                            cumulative_score += over_under
                            df.iat[row_idx - 1, SCORE_COL_START + d] = format_score(cumulative_score)
                            last_valid_score = over_under
                            day_scores[d].append(cumulative_score)
                            logging.info(f"{name} {col} using fallback SCORE: {over_under}")
                            found_any_scores = True
                            continue
                        except Exception as e:
                            logging.warning(f"Invalid fallback SCORE for {name}: {fallback}")
                            continue

                # Otherwise leave blank
                df.iat[row_idx - 1, SCORE_COL_START + d] = ''

            if cut:
                for future_day in range(d + 1, 4):
                    df.iat[row_idx - 1, SCORE_COL_START + future_day] = 'CUT'

        # Compute daily total if 3+ scores available
        total_row = start_row + 6
        total_day4 = None
        for d in range(4):
            scores = [v for v in day_scores[d] if isinstance(v, int)]
            if len(scores) >= 3:
                total = sum(sorted(scores)[:3])
                df.iat[total_row - 1, SCORE_COL_START + d] = format_score(total)
                if d == 3:
                    total_day4 = total
                logging.info(f'Day {d+1} total for {participant}: {format_score(total)}')
            else:
                df.iat[total_row - 1, SCORE_COL_START + d] = ''
                if d == 3:
                    total_day4 = None

        if total_day4 is not None:
            participant_totals_day4.append((participant, total_day4))

    if not found_any_scores:
        logging.info("⚠️ No completed rounds with scores found. Exiting without updating.")
        return

    # Rankings
    rankings = sorted(participant_totals_day4, key=lambda x: x[1])  # lower is better

    # Write winner at top
    winner_cell_row = PARTICIPANT_START_ROW - 2
    winner_cell_col = COLUMN_OFFSET
    winner_name = rankings[0][0] if rankings else ""
    df.iat[winner_cell_row - 1, winner_cell_col] = f"WINNER: {winner_name}"

    # Write top 3 below participants
    display_start_row = PARTICIPANT_START_ROW + BLOCK_SIZE * len(PARTICIPANTS) + 2
    rank_suffix = ['ST', 'ND', 'RD']
    for i, (name, score) in enumerate(rankings[:3]):
        rank_str = f"{i+1}{rank_suffix[i]}"
        display_text = f"{rank_str}: {name} ({format_score(score)})"
        df.iat[display_start_row - 1 + i, winner_cell_col] = display_text

    # Push to Google Sheets
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    logging.info('\n✅ Gamblor Scores Updated!')

if __name__ == '__main__':
    update_sheet()