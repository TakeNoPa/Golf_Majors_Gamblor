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
COLUMN_OFFSET = 20  # Column 'U'
SCORE_COL_START = 21  # Column 'V'
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

def round_in_progress(leaderboard, round_col):
    invalid_values = {'--', '', None, 'CUT', 'WD', 'DQ', '—'}
    if round_col not in leaderboard.columns:
        return False
    scores = leaderboard[round_col].astype(str).str.strip()
    num_valid = scores.apply(lambda x: x not in invalid_values).sum()
    total = len(scores)
    # In progress if some but not all have valid scores
    return 0 < num_valid < total

def round_complete(leaderboard, round_col):
    invalid_values = {'--', '', None, '—'}
    if round_col not in leaderboard.columns or 'SCORE' not in leaderboard.columns:
        return False

    scores = leaderboard[round_col].astype(str).str.strip()
    score_status = leaderboard['SCORE'].astype(str).str.strip().str.upper()

    # Round is complete if each player:
    # - Has a valid score
    # - OR is marked as CUT (so they won’t have a score)
    for s, status in zip(scores, score_status):
        if s in invalid_values and status != 'CUT':
            return False
    return True

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
    found_any_scores = False

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

                prev_col = ROUND_COLS[d-1] if d > 0 else None

                # Check if previous round is complete (or True for first round)
                if prev_col:
                    prev_round_complete = round_complete(leaderboard, prev_col)
                else:
                    prev_round_complete = True

                curr_round_complete = round_complete(leaderboard, col)
                curr_round_in_progress = round_in_progress(leaderboard, col)

                if not prev_round_complete:
                    # Can't process this round if previous not complete
                    df.iat[row_idx - 1, SCORE_COL_START + d] = ''
                    continue

                if not (curr_round_complete or curr_round_in_progress):
                    # Round hasn't started yet, leave blank
                    df.iat[row_idx - 1, SCORE_COL_START + d] = ''
                    continue

                val = lb_row[col]
                invalid_values = {'--', '', None, 'CUT', 'WD', 'DQ', '—'}

                if val not in invalid_values:
                    # Try to parse round score (int)
                    try:
                        s = int(val)
                        over_under = s - PAR
                        cumulative_score += over_under
                        df.iat[row_idx - 1, SCORE_COL_START + d] = format_score(cumulative_score)
                        last_valid_score = over_under
                        day_scores[d].append(cumulative_score)
                        found_any_scores = True
                        logging.info(f'{name} {col}: {s} → {format_score(over_under)}')
                        continue
                    except Exception:
                        pass

                # Round in progress but player's score missing or invalid; use fallback SCORE column
                if curr_round_in_progress:
                    fallback = lb_row.get('SCORE')
                    if isinstance(fallback, str):
                        try:
                            fb = fallback.strip().upper()
                            if fb == 'E':
                                over_under = 0
                            elif fb.startswith('+') or fb.startswith('-'):
                                over_under = int(fb)
                            else:
                                over_under = int(fb)
                            cumulative_score += over_under
                            df.iat[row_idx - 1, SCORE_COL_START + d] = format_score(cumulative_score)
                            last_valid_score = over_under
                            day_scores[d].append(cumulative_score)
                            found_any_scores = True
                            logging.info(f"{name} {col} using fallback SCORE (to-par): {over_under}")
                            continue
                        except Exception as e:
                            logging.warning(f"Could not parse fallback SCORE for {name}: {fallback}")
                            df.iat[row_idx - 1, SCORE_COL_START + d] = ''
                            continue
                    else:
                        df.iat[row_idx - 1, SCORE_COL_START + d] = ''
                        continue

                # Handle CUT in rounds 3 or 4
                if isinstance(val, str) and val.upper() == 'CUT' and d >= 2:
                    df.iat[row_idx - 1, SCORE_COL_START + d] = 'CUT'
                    cut = True
                    logging.info(f'{name} {col}: CUT')
                    break

                # No valid score to write
                df.iat[row_idx - 1, SCORE_COL_START + d] = ''

            if cut:
                for future_day in range(d + 1, 4):
                    df.iat[row_idx - 1, SCORE_COL_START + future_day] = 'CUT'

        # Compute totals: top 3 scores per day
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

    # Rankings based on day 4 totals (lower better)
    rankings = sorted(participant_totals_day4, key=lambda x: x[1])

    # Write winner at top
    winner_cell_row = PARTICIPANT_START_ROW - 2
    winner_cell_col = COLUMN_OFFSET
    winner_name = rankings[0][0] if rankings else ""
    df.iat[winner_cell_row - 1, winner_cell_col] = f"WINNER: {winner_name}"

    # Write top 3 below participants
    display_start_row = PARTICIPANT_START_ROW + BLOCK_SIZE * len(PARTICIPANTS) + 2
    rank_suffix = ['ST', 'ND', 'RD']
    for i, (name, score) in enumerate(rankings[:3]):
        rank_str = f"{i+1}{rank_suffix[i] if i < 3 else 'TH'}"
        display_text = f"{rank_str}: {name} ({format_score(score)})"
        df.iat[display_start_row - 1 + i, winner_cell_col] = display_text

    # Push to Google Sheets
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    logging.info('\n✅ Gamblor Scores Updated!')


if __name__ == '__main__':
    update_sheet()
