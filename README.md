# fa-essentials
A library with essentials needed in every backend python app. e.g. logging, local db connection, filtering, formatting etc.

## Sponsors
Freya Alpha,
The Kára System,
Spark & Hale Robotic Industries

## General
Run and compiled for Python 3.9.13.
Expected to run for Python 3+

## Development

### Testing
run tests with `pytest -s -vv` to see all the details.

### Installation as Consuming Developer

Simply run: `pip install fa-essentials`

Import in modules without the dash (e.g.): `from faessentials.globallogger import GlobalLogger`

### Setup as Contributor
Create the virtul environment: 
```
py -m venv .venv
```
Start the Environment: 
```
./.venv/Scripts/activate
```
 (or allow VS Code to start it). Use `deactivate`to stop it.

All the required libraries must be listed in requirements.txt and installed by  
```
python -m pip install -r .\requirements.txt
```
For Dev use 
```
python -m pip install -r .\requirements-dev.txt
```

To cleanup the environment run:
```
pip3 freeze > to-uninstall.txt
```
 and then
```
pip3 uninstall -y -r to-uninstall.txt
```

or 
```
pip3 install pip-autoremove
```

### Build Library
Prerequisite: make sure that you give your Operating System user the right to modify files in the python directory. The directory where pyhton is installed.
Use 
```python setup.py bdist_wheel```
 to create the dist, build and .eggs folder.

## Reference from a different project
In order to use your own version of the project - to maybe contribute to the library - simply clone the code from github into new directory. Then add the path of that new directory to the requirements.txt file of your project. Then change in fa-essentials whatever you recommend to improve. Don't forget the Open-Closed Principle: extend only (unless it requires a breaking change)


## Releasing a new version
The Release process is executed by github actions.