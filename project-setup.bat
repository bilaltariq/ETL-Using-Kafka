title Create a virtual environment

:: Create a virtual environment
@echo off
echo "Started"

set ENV_STEM="venv"
set ENV_NAME=%ENV_STEM%
::%ENV_NAME%
echo "An environment with the name: %ENV_NAME% will be created"

pip install virtualenv
python -m virtualenv %ENV_NAME%
echo "Virtual environment created"
call .\%ENV_NAME%\Scripts\activate.bat
echo "Virtual environment activated"
python -m ensurepip --upgrade
python -m pip install --upgrade pip
pip list
pip install -r config/requirements.txt
echo "Packages installed"
pip list
echo "Pull docker images used in this project."

docker pull wurstmeister/zookeeper:latest
docker pull wurstmeister/kafka:latest
docker pull bitnami/clickhouse:latest
docker pull mysql:latest

echo "All required Docker images have been pulled successfully."


code .
pause
