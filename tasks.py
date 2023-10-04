import pathlib
from src.util_invoke_tasks import *
import env


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


@task
def gcrdeploy(c):
    """
    Deploy the docker image to Google Cloud Run.
    :return:
    """
    print("Deploying docker image to Google Cloud Run...")
    tag = 'latest'
    docker_username = get_env_var('DOCKER_USERNAME')
    image_name = get_env_var('IMAGE_NAME')
    docker_tag = f'docker.io/{docker_username}/{image_name}:{tag}'
    envtoyaml(c)
    command = [
        'gcloud',
        'run',
        'deploy',
        get_env_var('IMAGE_NAME'),
        '--image',
        docker_tag,
        '--region',
        'us-east4',
        '--no-allow-unauthenticated',
        '--project',
        get_env_var('GCR_PROJECT_ID'),
        '--env-vars-file',
        './env.yaml',
        '--memory',
        '2Gi'
    ]
    print(' '.join(command))
    subprocess.run(command, check=True, shell=True)
