import os
from distutils.core import setup

packages = []
for root, dirs, files in os.walk('spire'):
    if '__init__.py' in files:
        packages.append(root.replace('/', '.'))

setup(
    name='spire',
    version='1.0.0a1',
    description='A component framework.',
    author='Jordan McCoy',
    author_email='mccoy.jordan@gmail.com',
    license='BSD',
    url='http://github.com/jordanm/spire',
    packages=packages,
    package_data={
        'spire.schema': ['templates/*'],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
