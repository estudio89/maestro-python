from setuptools import setup,find_packages
import os
import re

def read(f):
    return open(f, 'r', encoding='utf-8').read()

def get_version(package):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    init_py = open(os.path.join(package, '__init__.py')).read()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", init_py).group(1)


version = get_version('maestro')

setup(
	name="maestro",
	version=version,
	url='',
	license='BSD',
	description='Data synchronization framework.',
	long_description=read('README.md'),
	long_description_content_type='text/markdown',
	author='Luccas Correa',
	author_email='luccascorrea@estudio89.com.br',
	packages=find_packages(exclude=['tests*', 'example/']),
	include_package_data=True,
	install_requires=["python-dateutil>=2.8.1","filelock>3.7.1"],
    extras_require={
        "django": "django >= 3.1",
        "firestore": "firebase-admin >= 5.0.1",
        "mongo": "pymongo >= 3.12.0"
    },
	python_requires=">=3.8",
	classifiers=[
        'Environment :: Web Environment',
        'Framework :: Django',
        'Framework :: Django :: 2.2',
        'Framework :: Django :: 3.0',
        'Framework :: Django :: 3.1',
        'Framework :: Django :: 3.2',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3 :: Only',
        'Topic :: Internet :: WWW/HTTP',
	]
)