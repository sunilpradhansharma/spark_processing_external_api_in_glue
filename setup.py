from setuptools import setup, find_packages

setup(
    name="s3_api_processor",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        'requests==2.31.0',
        'ratelimit==2.2.1',
        'python-dotenv==1.0.0',
        'tqdm==4.66.1'
    ]
) 