from setuptools import find_packages, setup

setup(
    name='pybudget',
    version='0.5',
    description='Blah blah something budget in python',
    author='Redfern',
    author_email='wilson.fearn@gmail.com',
    url='https://www.github.com/wfearn/budget-cli',
    package_dir={'': 'src'},
    packages=find_packages('src')
)