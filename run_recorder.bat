@echo off
REM Agende no Agendador de Tarefas do Windows para dias úteis às 08:45.
REM Ajuste o caminho do projeto e do Python. As credenciais podem ficar em
REM variáveis de ambiente do usuário: PROFIT_KEY, PROFIT_USER, PROFIT_PASSWORD.
cd /d "%~dp0"
if not exist logs mkdir logs
python -m profit_recorder --config config.toml live >> "logs\live_%DATE:~-4%%DATE:~3,2%%DATE:~0,2%.log" 2>&1
