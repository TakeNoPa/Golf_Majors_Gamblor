import requests
import pandas as pd
import logging
import gspread
from google.oauth2.service_account import Credentials

# === Parameters ===
URL = 'https://www.espn.com/golf/leaderboard/_/tournamentId/401703516'
GOOGLE_SHEET_NAME = 'Golf_Majors_Gamblor'
SHEET_NAME = 'TOURNAMENT_LEADERBOARDS'
PAR = 70
COLUMN_OFFSET = 26  # Column 'AA' Add 6 Each Time (20, 26, 32)
SCORE_COL_START = 27  # Column 'AB'
BLOCK_SIZE = 7
PARTICIPANT_START_ROW = 229  # Row 230 in Excel (1-based)
ROUND_COLS = ['R1', 'R2', 'R3', 'R4']
PARTICIPANTS = ['PAT', 'TADGH / TADHG', 'MACKEY', 'HAYES', 'JOE', 'COOKE', 'FITZ']

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
    return best_match if best_ratio > 0.95 else None


def format_score(score):
    if score is None:
        return ''
    return 'E' if score == 0 else f'+{score}' if score > 0 else str(score)


def round_in_progress(leaderboard, round_col):
    invalid_values = {'--', '', None, 'CUT', 'WD', 'DQ', '—'}
    if round_col not in leaderboard.columns:
        return False

    scores = leaderboard[round_col].astype(str).str.strip()
    today_scores = leaderboard['TODAY'].astype(str).str.strip() if 'TODAY' in leaderboard.columns else pd.Series([''] * len(leaderboard))

    num_valid_in_round = scores.apply(lambda x: x not in invalid_values).sum()
    num_valid_in_today = today_scores.apply(lambda x: x not in invalid_values).sum()

    total = len(scores)
    # Round in progress if some scores present in round column or any valid scores in TODAY column
    return (0 < num_valid_in_round < total) or (num_valid_in_today > 0)


def round_complete(leaderboard, round_col):
    invalid_values = {'--', '', None, '—'}
    exit_codes = {'CUT', 'WD', 'DQ'}

    if round_col not in leaderboard.columns or 'SCORE' not in leaderboard.columns:
        logging.warning(f"Missing columns for round {round_col}.")
        return False

    round_scores = leaderboard[round_col].astype(str).str.strip().str.upper()
    statuses = leaderboard['SCORE'].astype(str).str.strip().str.upper()

    for i, (score, status) in enumerate(zip(round_scores, statuses)):
        if (score in invalid_values or score == 'CUT') and status not in exit_codes:
            logging.debug(
                f"❌ Incomplete round {round_col} due to player at row {i}: "
                f"score='{score}', status='{status}'"
            )
            return False

    logging.info(f"✅ Round {round_col} is marked COMPLETE.")
    return True


def update_winner_and_rankings(df, rankings):
    winner_cell_row = PARTICIPANT_START_ROW - 2
    winner_cell_col = COLUMN_OFFSET
    winner_name = rankings[0][0] if rankings else ""
    df.iat[winner_cell_row - 1, winner_cell_col] = f"WINNER: {winner_name}"

    display_start_row = PARTICIPANT_START_ROW + BLOCK_SIZE * len(PARTICIPANTS) + 2
    rank_suffix = ['ST', 'ND', 'RD']
    for i, (name, score) in enumerate(rankings[:3]):
        rank_str = f"{i+1}{rank_suffix[i] if i < 3 else 'TH'}"
        display_text = f"{rank_str}: {name} ({format_score(score)})"
        df.iat[display_start_row - 1 + i, winner_cell_col] = display_text


def process_golfer(match_name, row_idx, leaderboard, round_status):
    lb_row = leaderboard[leaderboard['PLAYER'] == match_name].iloc[0]
    round_deltas = []
    golfer_scores = {}
    cut = False
    found_score = False

    exit_codes = {'CUT', 'WD', 'DQ'}
    invalid_values = {'--', '', None, '—'}

    for d, col in enumerate(ROUND_COLS):
        if d > 0 and not round_status.get(ROUND_COLS[d - 1], False):
            df.iat[row_idx - 1, SCORE_COL_START + d] = ''
            continue

        val = str(lb_row.get(col, '')).strip().upper()
        status = str(lb_row.get('SCORE', '')).strip().upper()
        today_val = str(lb_row.get('TODAY', '')).strip().upper()

        if status in exit_codes:
            df.iat[row_idx - 1, SCORE_COL_START + d] = status
            cut = True
            break

        if val in exit_codes:
            df.iat[row_idx - 1, SCORE_COL_START + d] = val
            cut = True
            break

        curr_round_complete = round_status.get(col, False)
        curr_round_in_progress = not curr_round_complete and round_in_progress(leaderboard, col)

        if not curr_round_complete and not curr_round_in_progress:
            df.iat[row_idx - 1, SCORE_COL_START + d] = ''
            continue

        try:
            if curr_round_complete and val not in invalid_values:
                s = int(val)
                over_under = s - PAR
                round_deltas.append(over_under)
                cumulative_score = sum(round_deltas)
                df.iat[row_idx - 1, SCORE_COL_START + d] = format_score(cumulative_score)
                golfer_scores[d] = cumulative_score
                found_score = True
                continue

            elif curr_round_in_progress:
                score_to_use = today_val if d > 0 and val in invalid_values else (status if d == 0 else val)

                if score_to_use in exit_codes:
                    df.iat[row_idx - 1, SCORE_COL_START + d] = score_to_use
                    cut = True
                    break

                if score_to_use == 'E':
                    over_under = 0
                elif score_to_use.startswith(('+', '-')) or score_to_use.lstrip('-').isdigit():
                    over_under = int(score_to_use)
                else:
                    raise ValueError()

                round_deltas.append(over_under)
                cumulative_score = sum(round_deltas)
                df.iat[row_idx - 1, SCORE_COL_START + d] = format_score(cumulative_score)
                golfer_scores[d] = cumulative_score
                found_score = True
                continue

        except Exception:
            df.iat[row_idx - 1, SCORE_COL_START + d] = ''
            continue

        df.iat[row_idx - 1, SCORE_COL_START + d] = ''

    return golfer_scores, cut, found_score


def process_participant(participant, p_index, leaderboard, round_status):
    start_row = PARTICIPANT_START_ROW + p_index * BLOCK_SIZE
    day_scores = {i: [] for i in range(4)}
    found_any_scores = False

    for i in range(5):
        row_idx = start_row + 1 + i
        try:
            name = df.iat[row_idx - 1, COLUMN_OFFSET]
        except IndexError:
            continue

        match = find_best_match(name, leaderboard['PLAYER'].tolist())
        if not match:
            continue

        golfer_scores, cut, any_score = process_golfer(match, row_idx, leaderboard, round_status)
        found_any_scores = found_any_scores or any_score

        for d, score in golfer_scores.items():
            if isinstance(score, int):
                day_scores[d].append(score)
        if cut:
            for future_day in range(max(golfer_scores.keys(), default=0) + 1, 4):
                df.iat[row_idx - 1, SCORE_COL_START + future_day] = 'CUT'

    return day_scores, found_any_scores


def update_sheet():
    leaderboard = fetch_scores()
    if leaderboard is None:
        return

    leaderboard.columns = [col.upper() for col in leaderboard.columns]
    leaderboard_names = leaderboard['PLAYER'].tolist()
    sheet_data = sheet.get_all_values()
    global df
    df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    round_status = {r: round_complete(leaderboard, r) for r in ROUND_COLS}

    participant_totals_by_day = []
    found_any_scores = False

    for p_index, participant in enumerate(PARTICIPANTS):
        scores_by_day, any_scores = process_participant(participant, p_index, leaderboard, round_status)
        found_any_scores = found_any_scores or any_scores

        totals = {}
        for day, scores in scores_by_day.items():
            best_three = sorted(scores)[:3] if scores else []
            totals[day] = sum(best_three) if best_three else None

        participant_totals_by_day.append((participant, totals))

    if not found_any_scores:
        logging.info("No completed rounds with scores found. Exiting without updating.")
        return

    for p_index, (participant, totals) in enumerate(participant_totals_by_day):
        start_row = PARTICIPANT_START_ROW + p_index * BLOCK_SIZE
        for day in range(4):
            col_idx = SCORE_COL_START + day
            total_score = totals.get(day)
            if total_score is not None:
                df.iat[start_row - 1, col_idx] = format_score(total_score)
            else:
                df.iat[start_row - 1, col_idx] = ''

    participant_totals_day4 = []
    for participant, totals in participant_totals_by_day:
        total_day4 = totals.get(3)
        if total_day4 is not None:
            participant_totals_day4.append((participant, total_day4))

    rankings = sorted(participant_totals_day4, key=lambda x: x[1])

    update_winner_and_rankings(df, rankings)

    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    logging.info('Gamblor Scores Updated Successfully!')


if __name__ == '__main__':
    update_sheet()
