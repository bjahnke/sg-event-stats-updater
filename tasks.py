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
        f.write(
            '"""\n'
            'Desc:\n'
            'env_auto.py is generated from .env by the `invoke buildenvpy` task.\n'
            'it\'s purpose is to provide IDE support for environment variables.\n'
            '"""\n'
            '\n'
            'import os\n'
            'from dotenv import load_dotenv\n'
            'load_dotenv()\n\n'
            '\n'
        )
        for line in lines:
            if line != '\n' and not line.startswith('#'):
                variable = line.split('=')[0]
                f.write(f'{variable} = os.environ.get(\'{variable}\')\n')
    print('env/env_auto.py built from .env')


@task
def dockerrun(c):
    """
    run the docker image with IMAGE_NAME and .env variables
    :param c:
    :return:
    """
    print("Running docker image...")
    subprocess.run(["docker", "run", "--env-file", ".env", get_env_var("IMAGE_NAME")])


@task
def pytest(c):
    """
    run pytest
    :param c:
    :return:
    """
    print("Running pytest...")
    # python -m pytest test/
    subprocess.run(["py", "-m", "pytest", "test/"])
