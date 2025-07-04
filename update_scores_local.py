import requests
import pandas as pd
import logging
import re
import gspread
from google.oauth2.service_account import Credentials

# === Parameters ===
URL = 'https://www.espn.com/golf/leaderboard/_/tournamentId/401703518'
GOOGLE_SHEET_NAME = 'Golf_Majors_Gamblor'
SHEET_NAME = 'TOURNAMENT_LEADERBOARDS'
PAR = 71
COLUMN_OFFSET = 26  # Column numbers in Sheet -> (02 TPC)(08 Masters)(14 PGA)(20 US Open )(26 British Open)
SCORE_COL_START = 27  # Column for participants -> Add 1 to COLUMN_OFFSET
BLOCK_SIZE = 7
PARTICIPANT_START_ROW = 229  # Row 230 in Excel(PAT Row)
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

    name = name.strip().lower()
    leaderboard_names_clean = [lb_name.strip().lower() for lb_name in leaderboard_names]

    # First: try exact full-name match
    for lb_name in leaderboard_names_clean:
        if name == lb_name:
            return next(orig for orig in leaderboard_names if orig.lower().strip() == lb_name)

    # Fallback: fuzzy match on last name
    last_name = name.split(',')[0].strip() if ',' in name else name.split()[-1]
    best_match = None
    best_ratio = 0

    for lb_name in leaderboard_names:
        lb_last = lb_name.split(',')[0].strip() if ',' in lb_name else lb_name.split()[-1]
        ratio = difflib.SequenceMatcher(None, last_name.lower(), lb_last.lower()).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = lb_name

    return best_match if best_ratio > 0.9 else None

def format_score(score):
    return 'E' if score == 0 else f'+{score}' if score > 0 else str(score)

def is_start_time(value):
    """Returns True if the value looks like a tee time (e.g., '7:05 AM')."""
    return bool(re.match(r'^\d{1,2}:\d{2}\s?(AM|PM)$', value.strip(), re.IGNORECASE))

def propagate_exit_status(leaderboard, round_cols):
    # Define priority (lower index = higher priority)
    priority = ['WD', 'DQ', 'CUT']

    def get_best_exit_code(row):
        found_codes = [str(row.get(col, '')).upper() for col in round_cols]
        # Filter only exit codes present
        codes_present = [c for c in found_codes if c in priority]
        if not codes_present:
            return None
        # Return highest priority exit code
        for code in priority:
            if code in codes_present:
                return code
        return None

    leaderboard['EXIT_STATUS'] = leaderboard.apply(get_best_exit_code, axis=1)

    for col in round_cols:
        leaderboard.loc[leaderboard['EXIT_STATUS'].notna(), col] = leaderboard.loc[leaderboard['EXIT_STATUS'].notna(), 'EXIT_STATUS']

    leaderboard.drop(columns=['EXIT_STATUS'], inplace=True)
    return leaderboard

def round_in_progress(leaderboard, round_col):
    invalid_values = {'--', '', None, '—', '-'}
    exit_codes = {'CUT', 'WD', 'DQ'}
    allowed_thru_values = {'CUT', 'WD', 'DQ', '—'}

    # Identify all round columns (assuming naming like R1, R2, R3, R4)
    round_cols = [col for col in leaderboard.columns if col.startswith('R')]

    # Propagate exit codes before any further checks
    leaderboard = propagate_exit_status(leaderboard, round_cols)

    # Drop non-player rows based on PLAYER column (only letters, spaces, apostrophes, dashes, dots)
    leaderboard = leaderboard[leaderboard['PLAYER'].astype(str).str.match(r'^[A-Za-z\s\'\-\.]+$')]

    if round_col not in leaderboard.columns:
        logging.warning(f"Round column '{round_col}' not found in leaderboard.")
        return False

    if 'THRU' in leaderboard.columns:
        active_players = leaderboard[
            ~leaderboard['SCORE'].astype(str).str.upper().isin(exit_codes)
        ]

        thru_values = active_players['THRU'].astype(str).str.strip().str.replace('\u2014', '-', regex=False)

        def is_start_time(value):
            return bool(re.match(r'^\d{1,2}:\d{2}\s?(AM|PM)\*?$', value.strip(), re.IGNORECASE))

        bad_vals = [
            val for val in thru_values
            if val not in invalid_values and not is_start_time(val) and val.upper() not in allowed_thru_values
        ]

    scores = leaderboard[round_col].astype(str).str.strip()
    today_scores = leaderboard['TODAY'].astype(str).str.strip() if 'TODAY' in leaderboard.columns else pd.Series([''] * len(leaderboard))
    num_valid_in_round = scores.apply(lambda x: x not in invalid_values).sum()
    num_valid_in_today = today_scores.apply(lambda x: x not in invalid_values).sum()

    total = len(scores)
    in_progress = (0 < num_valid_in_round < total) or (num_valid_in_today > 0)

    if in_progress:
        logging.info("🏌️ Round appears to be in progress based on scores or TODAY column.")
    else:
        logging.info("🛑 No score activity detected — round not in progress.")

    return in_progress

def round_complete(leaderboard, round_col):
    """
    A round is complete only when:
    - All players have a score in that round OR
    - They have exited (CUT, WD, DQ)
    """
    invalid_values = {'--', '', None, '—', '-'}
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
    logging.info(f'✯ Round completion status: {round_status}')

    participant_totals_day4 = []
    found_any_scores = False

    for p_index, participant in enumerate(PARTICIPANTS):
        logging.info(f'\n✯ Processing participant: {participant}')
        scores_by_day, total_day4, any_scores = process_participant(participant, p_index, leaderboard, round_status)
        found_any_scores = found_any_scores or any_scores
        
        # Log participant’s day-by-day scores
        for day in range(4):
            scores = [s for s in scores_by_day.get(day, []) if isinstance(s, int)]
            if scores:
                scores_str = ', '.join(format_score(s) for s in scores)
                logging.info(f"Scores for {participant} on Day {day + 1}: {scores_str}")
            else:
                logging.info(f"No valid scores recorded for {participant} on Day {day + 1}")

        if total_day4 is not None:
            participant_totals_day4.append((participant, total_day4))

    if not found_any_scores:
        logging.info("⚠️ No completed rounds with scores found. Exiting without updating.")
        return

    logging.info("\n=== Participant totals after Day 4 ===")
    for name, total in participant_totals_day4:
        logging.info(f"{name}: {format_score(total)}")

    rankings = sorted(participant_totals_day4, key=lambda x: x[1])
    
    logging.info("\n=== Final Rankings ===")
    for i, (name, score) in enumerate(rankings, start=1):
        logging.info(f"{i}. {name} with score {format_score(score)}")

    if rankings:
        logging.info(f"🏆 Winner: {rankings[0][0]} with score {format_score(rankings[0][1])}")
    else:
        logging.info("No winner determined.")

    update_winner_and_rankings(df, rankings)

    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    logging.info('\n✅ Gamblor Scores Updated Successfully!')

def process_participant(participant, p_index, leaderboard, round_status):
    start_row = PARTICIPANT_START_ROW + p_index * BLOCK_SIZE
    day_scores = {i: [] for i in range(4)}
    found_any_scores = False
    total_day4 = None

    for i in range(5):
        row_idx = start_row + 1 + i
        try:
            name = df.iat[row_idx - 1, COLUMN_OFFSET]
        except IndexError:
            logging.warning(f'Missing golfer name at row {row_idx + 1}')
            continue

        match = find_best_match(name, leaderboard['PLAYER'].tolist())
        logging.info(f'Matched "{name}" -> "{match}"')
        if not match:
            logging.warning(f'No match found for {name}')
            continue

        golfer_scores, cut, any_score = process_golfer(match, row_idx, leaderboard, round_status)
        found_any_scores = found_any_scores or any_score

        for d, score in golfer_scores.items():
            if isinstance(score, int):
                day_scores[d].append(score)
        if cut:
            for future_day in range(max(golfer_scores.keys(), default=0) + 1, 4):
                df.iat[row_idx - 1, SCORE_COL_START + future_day] = 'CUT'

    # Total row
    total_row = start_row + 6
    for d in range(4):
        scores = [v for v in day_scores[d] if isinstance(v, int)]
        if len(scores) >= 3:
            total = sum(sorted(scores)[:3])
            df.iat[total_row - 1, SCORE_COL_START + d] = format_score(total)
            if d == 3:
                total_day4 = total
            logging.info(f'🏁 Day {d+1} total for {participant}: {format_score(total)}')
        else:
            df.iat[total_row - 1, SCORE_COL_START + d] = ''

    return day_scores, total_day4, found_any_scores

def process_golfer(match_name, row_idx, leaderboard, round_status):
    lb_row = leaderboard[leaderboard['PLAYER'] == match_name].iloc[0]
    cumulative_score = 0
    golfer_scores = {}
    found_score = False
    cut_applies_from_round = None
    has_exit_code = False
    exit_code_to_apply = None

    exit_codes = {'CUT', 'WD', 'DQ'}
    invalid_values = {'--', '', None, '—', '-'}

    # Check for exit code in SCORE column
    score_status = str(lb_row.get('SCORE', '')).strip().upper()
    if score_status in exit_codes:
        has_exit_code = True
        exit_code_to_apply = score_status

    for d, col in enumerate(ROUND_COLS):
        # Ensure previous round is complete before processing current
        if d > 0 and not round_status.get(ROUND_COLS[d - 1], False):
            logging.debug(f"Skipping {col} for {match_name} because {ROUND_COLS[d - 1]} is not complete.")
            df.iat[row_idx - 1, SCORE_COL_START + d] = ''
            continue

        val = str(lb_row.get(col, '')).strip().upper()
        today_val = str(lb_row.get('TODAY', '')).strip().upper()

        # If round value has an exit code, apply it and stop further processing
        if val in exit_codes:
            df.iat[row_idx - 1, SCORE_COL_START + d] = val
            logging.info(f'❌ {match_name} marked as {val} in {col} (from round column)')
            cut_applies_from_round = d
            has_exit_code = True
            exit_code_to_apply = val
            break

        curr_round_complete = round_status.get(col, False)
        curr_round_in_progress = not curr_round_complete and round_in_progress(leaderboard, col)

        if val not in invalid_values and val not in exit_codes:
            try:
                s = int(val)
                over_under = s - PAR
                cumulative_score += over_under
                df.iat[row_idx - 1, SCORE_COL_START + d] = format_score(cumulative_score)
                golfer_scores[d] = cumulative_score
                found_score = True
                logging.info(f'{match_name} {col}: Finished round score {s} → Over/Under {format_score(over_under)}, Cumulative {format_score(cumulative_score)}')
                continue
            except Exception:
                logging.warning(f'Error parsing finished score for {match_name} {col}: {val}')
                df.iat[row_idx - 1, SCORE_COL_START + d] = ''
                continue

        elif curr_round_in_progress:
            score_to_use = today_val if d > 0 else score_status

            if score_to_use in exit_codes:
                df.iat[row_idx - 1, SCORE_COL_START + d] = score_to_use
                cut_applies_from_round = d
                has_exit_code = True
                exit_code_to_apply = score_to_use
                logging.info(f'❌ {match_name} has status {score_to_use} in round {col} (from fallback score), marking as cut')
                break

            try:
                if score_to_use == 'E':
                    over_under = 0
                elif score_to_use.startswith(('+', '-')) or score_to_use.lstrip('-').isdigit():
                    over_under = int(score_to_use)
                else:
                    raise ValueError(f"Score format not recognized: '{score_to_use}'")

                cumulative_score += over_under
                df.iat[row_idx - 1, SCORE_COL_START + d] = format_score(cumulative_score)
                golfer_scores[d] = cumulative_score
                found_score = True
                logging.info(f'{match_name} {col}: In-progress score {score_to_use}, cumulative {format_score(cumulative_score)}')
                continue
            except Exception as ex:
                logging.warning(
                    f"⚠️ Invalid fallback score for {match_name} in round {col}: "
                    f"value='{score_to_use}' (source={'TODAY' if d > 0 else 'SCORE'}), error={ex}"
                )
                df.iat[row_idx - 1, SCORE_COL_START + d] = ''
                continue

        else:
            df.iat[row_idx - 1, SCORE_COL_START + d] = ''

        # If they have an exit code and haven't reached the round it applies to yet
        if has_exit_code and cut_applies_from_round is None:
            cut_applies_from_round = d
            break

    if has_exit_code:
        if cut_applies_from_round is not None:
            logging.info(f"🚨 {match_name} has status '{exit_code_to_apply}' in SCORE column — will apply from round {ROUND_COLS[cut_applies_from_round]}")
            for d in range(cut_applies_from_round, 4):
                df.iat[row_idx - 1, SCORE_COL_START + d] = exit_code_to_apply
            logging.info(f"❌ {match_name} marked as {exit_code_to_apply} from {ROUND_COLS[cut_applies_from_round]} onward")
        else:
            logging.info(f"ℹ️ {match_name} has status '{exit_code_to_apply}', but all available scores already recorded — no further rounds updated.")

    logging.info(f'✅ Processed {match_name}: ' +
                 ', '.join(f"{ROUND_COLS[d]}: {format_score(score)}" for d, score in golfer_scores.items()))

    return golfer_scores, has_exit_code, found_score


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

if __name__ == '__main__':
    update_sheet()
