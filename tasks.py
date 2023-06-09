import pathlib
from src.util_invoke_tasks import *
import env

@task
def buildenvpy(c):
    """
    Requires: .env in the root directory
    Build the env_auto.py file from the variables in .env using this pattern:
    import os and dotenv at the top of the file
    include new line, then:
    for each line in .env, if the line is not empty and does not start with '#', then
    split the line on '=' and use the variable to write a line in env_auto.py:
    <variable> = os.environ.get('<variable>')
    :param c:
    :return:
    """
    with open('.env', 'r') as f:
        lines = f.readlines()
    pathlib.Path('env').mkdir(parents=True, exist_ok=True)
    with open('env/env_auto.py', 'w') as f:
        f.write('import os\n')
        f.write('from dotenv import load_dotenv\n')
        f.write('load_dotenv()\n')
        f.write('\n')
        for line in lines:
            if line != '\n' and not line.startswith('#'):
                variable = line.split('=')[0]
                f.write(f'{variable} = os.environ.get(\'{variable}\')\n')
    print('env/env_auto.py built from .env')
