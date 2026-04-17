source .venv/bin/activate

### dam bao la da co csdl ms sql server

python app.py --merge-data

python app.py --merge-data --build-dw

python app.py --merge-data --build-dw --load-dw-data

python app.py --build-dw-cubes

python app.py --seed-dw-demo

python app.py --load-dw-cube-data

python app.py --run-api
