@echo off
:loop
python "C:\Users\obrie\Documents\Gamblor\update_scores_local.py"
timeout /t 300 /nobreak >nul
goto loop
